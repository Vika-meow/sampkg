#!/usr/bin/env bash


data_folder="../../../"
log_folder=${data_folder}sampled_data/log/
if [ ! -d ${log_folder} ];then
    mkdir -p ${log_folder}
    echo "create log folder: " ${log_folder}
fi
echo "data folder: " ${data_folder}
echo "log folder: " ${log_folder}
cur_time="`date +%Y%m%d%H%M%S`"


#target_dataset='DBP_en_WD_en_15K_V1'
#python3 main.py \
#    --target_dataset ${target_dataset} \
#    --KG1_rel_triple_path ${data_folder}'processed_data/rel_triples/rel_triples_DBP_en' \
#    --KG1_attr_triple_path ${data_folder}'processed_data/attr_triples/attr_triples_DBP_en' \
#    --KG2_rel_triple_path ${data_folder}'processed_data/rel_triples/rel_triples_WD_en' \
#    --KG2_attr_triple_path ${data_folder}'processed_data/attr_triples/attr_triples_WD_en' \
#    --ent_link_path ${data_folder}'processed_data/ent_links/ent_links_DBP_en_WD_en' \
#    --ent_link_num 15000 \
#    --init_speed 0.15 \
#    --pre_delete 0 >> ${data_folder}sampled_data/log/${target_dataset}_${cur_time}.log


target_dataset='DBP_en_YG_en_15K_V1'
python3 main.py \
    --target_dataset ${target_dataset} \
    --KG1_rel_triple_path ${data_folder}'processed_data/rel_triples/rel_triples_DBP_en' \
    --KG1_attr_triple_path ${data_folder}'processed_data/attr_triples/attr_triples_DBP_en' \
    --KG2_rel_triple_path ${data_folder}'processed_data/rel_triples/rel_triples_YG_en' \
    --KG2_attr_triple_path ${data_folder}'processed_data/attr_triples/attr_triples_YG_en' \
    --ent_link_path ${data_folder}'processed_data/ent_links/ent_links_DBP_en_YG_en' \
    --ent_link_num 15000 \
    --init_speed 0.15 \
    --pre_delete 2 >> ${data_folder}sampled_data/log/${target_dataset}_${cur_time}.log