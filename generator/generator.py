import random

from generator.entity_pagerank import PageRank
from others.utils import *
import generator.strategy


class Generator:

    def __init__(self, args, data):
        self.args = args
        self.triples_1, self.triples_2, self.ent_links = data.KG1_rel_triples, data.KG2_rel_triples, data.ent_links
        self.strategy = eval('generator.strategy.' + args.target_dataset)
        self.data = data
        self.generate_epoch = 1
        # self._generate_random()
        self._generate()

    def _generate_random(self):
        while len(self.ent_links) - self.args.ent_link_num > 150:
            delete_ratio = self.args.delete_random_ratio
            sample_num = int(len(self.ent_links)*(1-delete_ratio)+self.args.ent_link_num*delete_ratio)
            print('delete num:', len(self.ent_links)-sample_num)
            self.ent_links = set(random.sample(self.ent_links, sample_num))
            self.check_links()
            self.print_log(func_name='random')
        while len(self.ent_links) > self.args.ent_link_num:
            self.ent_links = set(random.sample(self.ent_links, len(self.ent_links)-1))
            self.check_links()
            self.print_log(func_name='random')
        self.sample_data = [self.triples_1, self.triples_2, self.ent_links]

    def _generate(self):
        args = self.args
        self.delete_limit = self.strategy['delete_limit']
        if args.init_speed != 0:
            self.delete_by_degree(is_print_log=True, delete_degree_ratio=args.init_speed)
        self.print_log(func_name='init')
        self.ent_links = set(random.sample(self.ent_links, int(len(self.ent_links)*(1-self.strategy['delete_random_ratio']))))
        self.check_links()
        self.print_log(func_name='random')
        while len(self.ent_links) / args.ent_link_num > 1:
            rate = len(self.ent_links) / args.ent_link_num
            is_print_log = False
            if '15K' in args.target_dataset and rate > 1.5 or rate > 1.05:
                self.print_log()
                is_print_log = True
            if rate > self.delete_limit:
                self.delete_by_degree_distribution()
                self.delete_by_degree(is_print_log=True)
            else:
                self.delete_by_degree(is_print_log=is_print_log)

            self.check_links()
        self.print_log(func_name='delete_by_degree')
        if self.strategy['preserve_num'] != 0:
            self.preserve_high_degree_entity()
            self.preserve_high_degree_entity(is_change_position=True)
        while len(self.ent_links) / args.ent_link_num > 1:
            self.delete_by_degree(is_print_log=False)
        if 'YG_en_15K_V2' in args.target_dataset:
            self.triples_2 = delete_relation_yg(self.triples_2, self.data.ddo2, is_15K=True)
        if 'YG_en_100K_V2' in args.target_dataset:
            self.triples_2 = delete_relation_yg(self.triples_2, self.data.ddo2)
        self.print_log(func_name='delete_by_degree')

        self.sample_data = [self.triples_1, self.triples_2, self.ent_links]
        print(self.strategy)
        return

    def preserve_high_degree_entity(self, is_change_position=False):
        preserve_num = self.strategy['preserve_num']
        ents1 = set([e for (e, _, _) in self.triples_1]) | set([e for (_, _, e) in self.triples_1])
        ents2 = set([e for (e, _, _) in self.triples_2]) | set([e for (_, _, e) in self.triples_2])

        if is_change_position:
            ents_preserve2 = set()
            for e in self.data.high_degree_ents2:
                if e not in ents2:
                    ents_preserve2.add(e)
                if len(ents_preserve2) >= preserve_num:
                    break
            links_preserve = set([(e1, e2) for (e1, e2) in self.data.ent_links if e2 in ents_preserve2])
            ents1_temp = set([e for (e, _) in links_preserve])
            ents_preserve1 = set()
            for e in ents1_temp:
                if e not in ents1:
                    ents_preserve1.add(e)
            ents_preserve2 = set([e2 for (e1, e2) in links_preserve if e1 in ents_preserve1])
        else:
            ents_preserve1 = set()
            for e in self.data.high_degree_ents1:
                if e not in ents1:
                    ents_preserve1.add(e)
                if len(ents_preserve1) >= preserve_num:
                    break
            links_preserve = set([(e1, e2) for (e1, e2) in self.data.ent_links if e1 in ents_preserve1])
            ents2_temp = set([e for (_, e) in links_preserve])
            ents_preserve2 = set()
            for e in ents2_temp:
                if e not in ents2:
                    ents_preserve2.add(e)
            ents_preserve1 = set([e1 for (e1, e2) in links_preserve if e2 in ents_preserve2])

        ents1 = ents1 | ents_preserve1
        ents2 = ents2 | ents_preserve2

        self.triples_1 = set([(h, r, t) for (h, r, t) in self.data.KG1_rel_triples if h in ents1 and t in ents1])
        self.triples_2 = set([(h, r, t) for (h, r, t) in self.data.KG2_rel_triples if h in ents2 and t in ents2])
        self.ent_links = set([(e1, e2) for (e1, e2) in self.data.ent_links if e1 in ents1 and e2 in ents2])
        self.print_log(func_name='preserve_high_degree_entity')
        return

    def delete_by_degree(self, min_priority=True, is_print_log=True, delete_degree_ratio=None):
        if delete_degree_ratio is None:
            delete_degree_ratio = self.strategy['delete_degree_ratio']
        if delete_degree_ratio == 0:
            return
        size = len(self.ent_links) - self.args.ent_link_num
        delete_degree_num = max(int(size * delete_degree_ratio), 1)
        ents1_sorted = count_ent_degree(self.triples_1, is_sorted=True)
        ents2_sorted = count_ent_degree(self.triples_2, is_sorted=True)
        if min_priority:
            ents1_to_delete = set(ents1_sorted[-delete_degree_num:])
            ents2_to_delete = set()
            if size > 5:
                ents2_to_delete = set(ents2_sorted[-delete_degree_num:])
        else:
            ents1_sorted = ents1_sorted[-delete_degree_num * 10:]
            ents2_sorted = ents2_sorted[-delete_degree_num * 10:]
            random.shuffle(ents1_sorted)
            random.shuffle(ents2_sorted)
            ents1_to_delete = set(ents1_sorted[-delete_degree_num:])
            ents2_to_delete = set(ents2_sorted[-delete_degree_num:])
        self.update_triples_and_links(ents1_to_delete, ents2_to_delete)
        if is_print_log:
            self.print_log('delete_by_degree')
        return

    def delete_by_degree_distribution(self):
        delete_dd_ratio = self.strategy['delete_ratio']
        if delete_dd_ratio <= 0:
            return
        ents1_to_delete = self.delete_by_pagerank_for_dd(self.triples_1, self.data.ddo1, delete_dd_ratio,
                                                         self.strategy['max_degree_kg1'])
        ents2_to_delete = self.delete_by_pagerank_for_dd(self.triples_2, self.data.ddo2, delete_dd_ratio,
                                                         self.strategy['max_degree_kg2'])
        self.update_triples_and_links(ents1_to_delete, ents2_to_delete)
        self.print_log(func_name='delete_by_degree_distribution')
        return

    def delete_by_pagerank_for_dd(self, triples, ddo, delete_dd_ratio, max_degree):
        ents_to_delete = set()
        ents_pr = PageRank(triples).page_rank
        ddc, degree_ents_dict = count_degree_distribution(triples, max_degree)
        delete_random_ratio = self.strategy['delete_random_ratio']
        print(format_print_dd(ddo, prefix='\t'))
        print(format_print_dd(ddc, prefix='\t'))
        for d, ents in degree_ents_dict.items():
            size = len(ents)
            if size == 0:
                continue
            delete_dd_num = int(size * delete_dd_ratio * (1 + ddc[d] - ddo[d]))
            if d < 8 and ddc[d] > ddo[d]:
                delete_dd_num = int(size * delete_dd_ratio * 3 * (1 + ddc[d] - ddo[d]))
            delete_random_num = int(delete_dd_num * delete_random_ratio)
            ents_to_delete_random = set()
            if delete_random_num < size:
                ents_to_delete_random = set(random.sample(ents, delete_random_num))

            cnt = delete_random_num
            for e in ents_pr:
                if cnt >= delete_dd_num:
                    break
                if e in ents and e not in ents_to_delete_random:
                    ents_to_delete.add(e)
                    cnt += 1
            ents_to_delete.update(ents_to_delete_random)
        return ents_to_delete

    def update_triples_and_links(self, ents1_to_delete, ents2_to_delete):
        if self.args.open_dataset == 0:
            self.triples_1 = set([(h, r, t) for (h, r, t) in self.triples_1
                                  if h not in ents1_to_delete and t not in ents1_to_delete])
            self.triples_2 = set([(h, r, t) for (h, r, t) in self.triples_2
                                  if h not in ents2_to_delete and t not in ents2_to_delete])
        else:
            self.triples_1 = set([(h, r, t) for (h, r, t) in self.triples_1 if h not in ents1_to_delete])
            self.triples_2 = set([(h, r, t) for (h, r, t) in self.triples_2 if h not in ents2_to_delete])
        self.ent_links = set([(e1, e2) for (e1, e2) in self.ent_links if e1 not in ents1_to_delete
                              and e2 not in ents2_to_delete])
        self.check_links()
        return

    def check_links(self):
        """
        It's no necessary to check entity links every epoch.
        :return:
        """
        ents1_lk = set([e for (e, _) in self.ent_links])
        ents2_lk = set([e for (_, e) in self.ent_links])
        ents1_tr = set([e for (e, _, _) in self.triples_1])
        ents2_tr = set([e for (e, _, _) in self.triples_2])
        if self.args.open_dataset == 0:
            ents1_tr = ents1_tr | set([e for (_, _, e) in self.triples_1])
            ents2_tr = ents2_tr | set([e for (_, _, e) in self.triples_2])
        ents1 = ents1_lk & ents1_tr
        ents2 = ents2_lk & ents2_tr
        stop = len(ents1_lk - ents1_tr) == len(ents1_tr - ents1_lk) == len(ents2_lk - ents2_tr) == len(
            ents2_tr - ents2_lk) == 0
        if stop:
            return
        self.ent_links = set([(e1, e2) for (e1, e2) in self.ent_links if e1 in ents1 and e2 in ents2])
        if self.args.open_dataset == 0:
            self.triples_1 = set([(h, r, t) for (h, r, t) in self.triples_1 if h in ents1 and t in ents1])
            self.triples_2 = set([(h, r, t) for (h, r, t) in self.triples_2 if h in ents2 and t in ents2])
        else:
            self.triples_1 = set([(h, r, t) for (h, r, t) in self.triples_1 if h in ents1])
            self.triples_2 = set([(h, r, t) for (h, r, t) in self.triples_2 if h in ents2])
        self.check_links()

    def print_log(self, func_name=None):
        if func_name is None:
            print('\niteration:', self.generate_epoch)
            self.generate_epoch += 1
        else:
            ents1 = set([e for (e, _, _) in self.triples_1]) | set([e for (_, _, e) in self.triples_1])
            ents2 = set([e for (e, _, _) in self.triples_2]) | set([e for (_, _, e) in self.triples_2])
            print('\t' + func_name, ':')
            print('\tentity_num_1:', len(ents1))
            print('\tentity_num_2:', len(ents2))
            print('\ttriple_num_1:', len(self.triples_1))
            print('\ttriple_num_2:', len(self.triples_2))
            print('\tlink_num:', len(self.ent_links), '\n')
        return
