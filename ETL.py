
# coding: utf-8

# load from hive(walmart) and save new vertices and edges as hive in shanghai(50)


import pandas as pd
from sklearn.externals import joblib
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import Row


#UDFs
def split_pair(line):
    line_split=line.split('|')
    x0=line_split[0]
    x1=line_split[1]
    x2=line_split[2]
    return (x0,x1),(x0,x2),(x1,x2)

def mod(x):
    x=str(x)
    l=len(x)
    x='u'+'0'*(9-l)+x
    return x

def proc_null(line):
    if line[0]=='null'or line[0]=='':
        return line[1],line[1]
    elif line[1]=='null' or line[1]=='':
        return line[0],line[0]
    else:
        return line[0],line[1]

def main():
    spark = SparkSession.builder.appName("cc_in_out").enableHiveSupport().getOrCreate()
    #sc = spark.sparkContext
    ## 读取数据源
    spark.sql("use cn_ods_checkoutreports")
    df1 = spark.sql("select user_id,mobile_phone from o_bas_checkout_users")
    checkout = df1.rdd.map(lambda line:line['user_id'] + "|" + line['mobile_phone'])

    spark.sql("use cn_ods_mobilereports")
    df2 = spark.sql("select thirdparty_id,user_id,union_id from o_bas_vmobile_thirdparty_d")
    ghs_2 = df2.rdd.map(lambda line:line['thirdparty_id'] + "|" + line['user_id'] + "|" +line['union_id'])

    spark.sql("use cn_ods_miniprogram")
    df3 = spark.sql("select openid,unionid,mobile from o_bas_weixin_user_d")
    ghs = df3.rdd.map(lambda line:line['openid'] + "|" + line['unionid'] + "|" + line['mobile'])

    spark.sql("use cn_ods_apiwmcnwechatwalmart")
    df4 = spark.sql("select openid,unionid from o_api_user_info_d")
    wfans = df4.rdd.map(lambda line:line['openid'] + "|" + line['unionid'])

    spark.sql("use cn_ods_apiwmcnwechatsams")
    df5 = spark.sql("select openid,unionid from o_api_user_info_d")
    wmcnwechatsams = df5.rdd.map(lambda line:line['openid'] + "|" + line['unionid'])

    spark.sql("use JDDJReports")
    df6 = spark.sql("select user_id,delivery_phone from o_bas_oms_ordhdr_d")
    jddj = df6.rdd.map(lambda line:line['user_id'] + "|" + line['delivery_phone'])

    #jddj = sc.textFile("/opt2/connected_components/uid/jddj_userid_phone.txt")
    #ghs = sc.textFile("/opt2/connected_components/uid/ghs_mini_openid_unionid_phone.txt")
    #checkout = sc.textFile("/opt2/connected_components/uid/checkout_userid_phone.txt")
    #ghs_2 = sc.textFile("/opt2/connected_components/uid/ghs_openid_userid_unionid.txt")
    #wmcnwechatsams = sc.textFile("/opt2/connected_components/uid/wmcnwechatsams_openid_unionid.txt")
    #wfans = sc.textFile("/opt2/connected_components/uid/wfans_openid_unionid.txt")

    ## 生成 （ID, type）
    jddj_new = jddj.map(lambda x: ((x.split('|')[0], 'userid'), (x.split('|')[1], 'phone'))).flatMap(lambda x: x)
    ghs_type = ghs.map(lambda x: (
    (x.split('|')[0], 'ghs_mini_openid'), (x.split('|')[1], 'unionid'), (x.split('|')[2], 'phone'))).flatMap(
        lambda x: x)
    checkout_type = checkout.map(lambda x: ((x.split('|')[0], 'userid'), (x.split('|')[1], 'phone'))).flatMap(
        lambda x: x)
    ghs_2_type = ghs_2.map(
        lambda x: ((x.split('|')[0], 'ghs_openid'), (x.split('|')[1], 'userid'), (x.split('|')[2], 'unionid'))).flatMap(
        lambda x: x)
    sams_type = wmcnwechatsams.map(lambda x: ((x.split('|')[0], 'sams_openid'), (x.split('|')[1], 'unionid'))).flatMap(
        lambda x: x)
    wfans_type = wfans.map(lambda x: ((x.split('|')[0], 'wfans_openid'), (x.split('|')[1], 'unionid'))).flatMap(
        lambda x: x)
    all_type = ghs_type.union(checkout_type).union(ghs_2_type).union(sams_type).union(wfans_type)

    all_type = all_type.map(lambda x: (x[0].strip(), x[1])).filter(lambda x: x[0] != 'null').filter(
        lambda x: x[0] != '').distinct()

    ## 生成pairs (ID,ID)
    ghs_new = ghs.map(split_pair).flatMap(lambda x: x)
    jddj_new = jddj.map(lambda x: (x.split('|')[0], x.split('|')[1]))
    checkout_new = checkout.map(lambda x: (x.split('|')[0], x.split('|')[1]))
    ghs_2_new = ghs_2.map(split_pair).flatMap(lambda x: x)
    wmcnwechatsams_new = wmcnwechatsams.map(lambda x: (x.split('|')[0], x.split('|')[1]))
    wfans_new = wfans.map(lambda x: (x.split('|')[0], x.split('|')[1]))

    all_pairs = ghs_new.union(checkout_new).union(ghs_2_new).union(wmcnwechatsams_new).union(wfans_new).union(jddj_new)
    all_pairs = all_pairs.map(lambda x: (x[0].strip(), x[1].strip())).map(proc_null)

    ## 生成序号（index,ID）
    #vertices_raw = all_pairs.flatMap(lambda x: x).filter(lambda x: x != 'null').filter(
    #    lambda x: x != '').distinct().zipWithIndex()
    ### 给顶点添加properties
    #vertices = vertices_raw.map(lambda x: (str(x[1]) + '|' + mod(x[1]) + '~' + x[0] + '~' + 'open_id' + '|ver:'))
    #vertices.saveAsTextFile('/opt2/connected_components/official_test/vertices')

    ## (index,id)
    vertices_index = all_type.zipWithIndex()
    ## save to uid_new_vertices
    vertices_new = vertices_index.map(lambda x: Row(index=str(x[1]), cname=mod(x[1]), id=x[0][0], type=x[0][1]))
    vertices_new_df = spark.createDataFrame(vertices_new)
    vertices_new_df.createOrReplaceTempView("mytempver")
    spark.sql("use cn_omni_uid")
    spark.sql("drop table if exists uid_new_vertices")
    spark.sql("create table uid_new_vertices as select * from mytempver")


    ## 将pairs里面的ID 替换成index
    ## version怎么递增？？？
    #edges = all_pairs.join(vertices_index).map(lambda x: x[1]).join(vertices_index).map(
    #    lambda x: str(x[1][0]) + ' ' + str(x[1][1]) + ' version_1')

    #edges.saveAsTextFile('/opt2/connected_components/official_test/edges')

    edges = all_pairs.join(vertices_index).map(lambda x: x[1]).join(vertices_index).map(
        lambda x: Row(v1=str(x[1][0], v2=str(x[1][1]), version="version_1")))
    edges_new_df = spark.createDataFrame(edges)
    edges_new_df.createOrReplaceTempView("mytempedg")
    spark.sql("use cn_omni_uid")
    spark.sql("drop table if exists uid_new_edges")
    spark.sql("create table uid_new_edges as select * from mytempedg")



if __name__ == '__main__':
    main()




