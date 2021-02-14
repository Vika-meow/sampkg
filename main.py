import argparse
import time
import gc

from generator.generator import *
from data_processor.data_model import *
from generator.discriminator import *

parser = argparse.ArgumentParser(description='DataGenerator')

KG1, KG2 = 'en', 'ru'
folder_input = '../dataset/'

#
target_dataset = 'DBP_' + KG1 + '_DBP_' + KG2 + '_15K_V2'

ent_link_num = 15000
if '100K' in target_dataset:
    ent_link_num = 100000
else:
    ent_link_num = 1000000
# 1,000,000

parser.add_argument('--target_dataset', type=str, default=target_dataset)
parser.add_argument('--dataset_sample_num', type=int, default=1)
parser.add_argument('--ent_link_num', type=int, default=ent_link_num)

parser.add_argument('--KG1_rel_triple_path', type=str, default=folder_input+'rel_triplets_'+KG1)
parser.add_argument('--KG2_rel_triple_path', type=str, default=folder_input+'rel_triplets_'+KG2)
# parser.add_argument('--KG1_rel_triple_path', type=str, default=folder_input+'rel_triples/rel_triples_' + KG1 + '_V2')
# parser.add_argument('--KG2_rel_triple_path', type=str, default=folder_input+'rel_triples/rel_triples_' + KG2 + '_V2')

parser.add_argument('--KG1_attr_triple_path', type=str, default=folder_input+'attr_triplets_' + KG1)
parser.add_argument('--KG2_attr_triple_path', type=str, default=folder_input+'attr_triplets_' + KG2)
parser.add_argument('--ent_link_path', type=str, default=folder_input+'ent_links_' + KG1 + '_' + KG2)

parser.add_argument('--output_folder', type=str, default='../sampled_data_rebuttal/')
parser.add_argument('--dataset_division', type=str, default='721')
parser.add_argument('--draw_degree_distribution', type=bool, default=True)

parser.add_argument('--js_expectation', type=float, default=0.995)
parser.add_argument('--init_speed', type=float, default=0)
parser.add_argument('--delete_ratio', type=float, default=0.01)
parser.add_argument('--delete_random_ratio', type=float, default=0.05)
parser.add_argument('--delete_degree_ratio', type=float, default=0.01)
parser.add_argument('--delete_limit', type=int, default=1000000)
parser.add_argument('--preserve_num', type=int, default=50)
parser.add_argument('--max_degree_kg1', type=int, default=100)
parser.add_argument('--max_degree_kg2', type=int, default=100)
parser.add_argument('--pre_delete', type=int, default=0)  # 0, 1, 2, 3
parser.add_argument('--open_dataset', type=int, default=0)  # 0: close_dataset, head entity + tail entity = entity
                                                            # 1: open_dataset, head entity = entity

# data_format_conversion
# parser.add_argument('--data_format_conversion', type=bool, default=False)
# parser.add_argument('--original_data_folder', type=str, default='')
# parser.add_argument('--conversion_result_folder', type=str, default='')

args = parser.parse_args()
print(args)

if __name__ == '__main__':
    data = DataModel(args)
    discriminator = Discriminator(args, [data.ddo1, data.ddo2])
    dataset_sample_index = 1
    wrong_num = 0
    while True:
        start_time = time.time()
        print('\nsample:', dataset_sample_index)
        g = Generator(args, data)
        sample_data = g.sample_data
        print('this sample run time before write_generated_data: %.2f min\n' % ((time.time() - start_time) / 60))
        if discriminator.accept_or_reject(sample_data):
            data.write_generated_data(sample_data, dataset_sample_index)
            dataset_sample_index += 1
        else:
            data.draw(sample_data, './fig/')
            wrong_num += 1
        # data.write_generated_data(sample_data, dataset_sample_index)
        # break
        del g
        gc.collect()
        print('this sample finish time:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('this sample run time: %.2f min\n' % ((time.time() - start_time) / 60))
        if dataset_sample_index > args.dataset_sample_num:
            break
        if wrong_num >= 1:
            break
