import random

from data_processor.file_io import *
from others.utils import *
import generator.strategy


class DataModel:

    def __init__(self, args):
        self.args = args
        self.strategy = eval('generator.strategy.' + args.target_dataset)
        self._init()

    def _init(self):
        args = self.args
        self.ent_links = read_links(args.ent_link_path)
        ents_kg1 = set([e for (e, _) in self.ent_links])
        ents_kg2 = set([e for (_, e) in self.ent_links])

        KG1_rel_triples_raw = read_triples(args.KG1_rel_triple_path)
        KG2_rel_triples_raw = read_triples(args.KG2_rel_triple_path)

        self.ddo1, _ = count_degree_distribution(KG1_rel_triples_raw, self.strategy['max_degree_kg1'])
        self.ddo2, _ = count_degree_distribution(KG2_rel_triples_raw, self.strategy['max_degree_kg2'])
        open_dataset = False
        if args.open_dataset == 1:
            open_dataset = True

        KG1_rel_triples_raw = filter_rel_triples_by_ents(KG1_rel_triples_raw, ents_kg1, open_dataset=open_dataset)
        KG2_rel_triples_raw = filter_rel_triples_by_ents(KG2_rel_triples_raw, ents_kg2, open_dataset=open_dataset)

        if self.strategy['preserve_num'] != 0:
            _, degree_ents_dict1 = count_degree_distribution(KG1_rel_triples_raw, self.strategy['max_degree_kg1'])
            _, degree_ents_dict2 = count_degree_distribution(KG2_rel_triples_raw, self.strategy['max_degree_kg2'])
            self.high_degree_ents1 = set(degree_ents_dict1[self.strategy['max_degree_kg1']])
            self.high_degree_ents2 = set(degree_ents_dict2[self.strategy['max_degree_kg2']])

        self.KG1_rel_triples = filter_rel_triples_by_ents(KG1_rel_triples_raw, ents_kg1, open_dataset=open_dataset)
        self.KG2_rel_triples = filter_rel_triples_by_ents(KG2_rel_triples_raw, ents_kg2, open_dataset=open_dataset)

        if args.pre_delete == 1 or args.pre_delete == 3:
            ents_kg1 = self.pre_kg(self.KG1_rel_triples)
            self.KG1_rel_triples = filter_rel_triples_by_ents(self.KG1_rel_triples, ents_kg1, open_dataset=open_dataset)
        if args.pre_delete == 2 or args.pre_delete == 3:
            delete_ratio = 1.0
            if 'DBP_en_YG_en_15K_V1' in args.target_dataset:
                delete_ratio = 0.95
            ents_kg2 = self.pre_kg(self.KG2_rel_triples, delete_ratio=delete_ratio)
            self.KG2_rel_triples = filter_rel_triples_by_ents(self.KG2_rel_triples, ents_kg2, open_dataset=open_dataset)

        self.KG1_attr_triples = filter_attr_triples_by_ents(read_triples(args.KG1_attr_triple_path), ents_kg1)
        self.KG2_attr_triples = filter_attr_triples_by_ents(read_triples(args.KG2_attr_triple_path), ents_kg2)

        print('rel_triples:', len(self.KG1_rel_triples), len(self.KG2_rel_triples))
        print('attr_triples:', len(self.KG1_attr_triples), len(self.KG2_attr_triples))
        print('entities:', len(ents_kg1), len(ents_kg2))
        return

    def write_generated_data(self, sample_data, sample_index):
        rel_triples_1, rel_triples_2, ent_links = sample_data[0], sample_data[1], sample_data[2]

        output_folder = self.args.output_folder + str(sample_index) + '/'
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        output_folder += self.args.target_dataset + '/'
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)

        if self.args.draw_degree_distribution:
            self.draw(sample_data, output_folder)

        ents_kg1 = set([e for (e, _) in ent_links])
        ents_kg2 = set([e for (_, e) in ent_links])

        write_links(output_folder + 'ent_links', ent_links)
        split_and_write_entity_links(ent_links, output_folder)

        write_triples(output_folder + 'rel_triples_1', rel_triples_1)
        write_triples(output_folder + 'rel_triples_2', rel_triples_2)
        write_triples(output_folder + 'attr_triples_1', filter_attr_triples_by_ents(self.KG1_attr_triples, ents_kg1))
        write_triples(output_folder + 'attr_triples_2', filter_attr_triples_by_ents(self.KG2_attr_triples, ents_kg2))
        return

    @staticmethod
    def pre_kg(triples, delete_ratio=1.0):
        _, degree_ents_dict = count_degree_distribution(triples, 100)
        degree_ents = list(degree_ents_dict.values())
        ents_to_sample = set()
        for i in range(-10, 0):
            ents_to_sample = ents_to_sample | set(degree_ents[i])
        ents_to_delete = set(random.sample(ents_to_sample, int(len(ents_to_sample)*delete_ratio)))
        ents = set([h for (h, _, _) in triples]) | set([t for (_, _, t) in triples])
        return ents - ents_to_delete

    def draw(self, sample_data, output_folder):
        rel_triples_1, rel_triples_2 = sample_data[0], sample_data[1]

        dd_sample1, _ = count_degree_distribution(rel_triples_1, 100)
        dd_sample2, _ = count_degree_distribution(rel_triples_2, 100)

        cdf_kg1 = count_cdf(self.ddo1, 100)
        cdf_kg2 = count_cdf(self.ddo2, 100)
        cdf_sample1 = count_cdf(dd_sample1, 100)
        cdf_sample2 = count_cdf(dd_sample2, 100)
        print(format_print_dd(self.ddo1, prefix='dd_kg1:    \t'))
        print(format_print_dd(dd_sample1, prefix='dd_sample1:\t'))
        print(format_print_dd(self.ddo2, prefix='dd_kg2:    \t'))
        print(format_print_dd(dd_sample2, prefix='dd_sample2:\t'))
        draw_fig([cdf_kg1, cdf_sample1, cdf_kg2, cdf_sample2],
                 ['source KG 1', 'sampled data_processor 1', 'source KG 2', 'sampled data_processor 2'],
                 [output_folder+'cdf_' + self.args.target_dataset, 'degree', 'cdf'])
        draw_fig([self.ddo1, dd_sample1, self.ddo2, dd_sample2],
                 ['source KG 1', 'sampled data_processor 1', 'source KG 2', 'sampled data_processor 2'],
                 [output_folder+'degree_distribution_' + self.args.target_dataset, 'degree', 'degree distribution'],
                 limit=[0, 20, 0, 0.4])
        return
