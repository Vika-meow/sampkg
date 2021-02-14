"""
Discriminator:

"""
from others.utils import *
import generator.strategy


class Discriminator:
    def __init__(self, args, ddos):
        self.args = args
        self.strategy = eval('generator.strategy.' + args.target_dataset)
        self.max_degree1, self.max_degree2 = self.strategy['max_degree_kg1'], self.strategy['max_degree_kg2']
        self.ddo1, self.ddo2 = ddos[0], ddos[1]

    def accept_or_reject(self, sample_data):
        sample_triples_1, sample_triples_2, sample_ent_links = sample_data[0], sample_data[1], sample_data[2]

        ents1 = set([e for (e, _, _) in sample_triples_1]) | set([e for (_, _, e) in sample_triples_1])
        ents2 = set([e for (e, _, _) in sample_triples_2]) | set([e for (_, _, e) in sample_triples_2])
        ent_num = self.args.ent_link_num

        sim_kg1 = js_divergence(self.ddo1, sample_triples_1, self.max_degree1)
        sim_kg2 = js_divergence(self.ddo2, sample_triples_2, self.max_degree2)
        print('sim_kg1:', sim_kg1)
        print('sim_kg2:', sim_kg2)
        print('len(ents1):', len(ents1))
        print('len(ents2):', len(ents2))

        if len(ents1) != ent_num or len(ents2) != ent_num or len(sample_ent_links) != ent_num:
            print('reject: num wrong')
            return False

        if sim_kg1 > self.args.js_expectation or sim_kg2 > self.args.js_expectation:
            print('reject: JS divergence not in '+str(self.args.js_expectation))
            return False

        print('accept!')
        return True
