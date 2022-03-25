DROP TABLE IF EXISTS page_view;
CREATE TABLE page_view (
                           product_id       INT,
                           sex              INT,
                           province         STRING,
                           full_name        STRING,
                           click_time       TIMESTAMP(3),
                           WATERMARK FOR click_time AS click_time - INTERVAL '4' SECOND
) WITH (
      'connector' = 'faker',                                                              -- 必选参数,固定值为faker
      'fields.product_id.expression'    = '#{number.numberBetween ''1'',''100''}',
      'fields.sex.expression' = '#{regexify ''(0|1){1}''}',                               -- 针对sex字段随机生成0、1两种值，用于后续通过性别统计
      'fields.province.expression'  = '#{regexify ''(河北省|山西省|辽宁省|吉林省|黑龙江省|江苏省|浙江省|安徽省|福建省|江西省|山东省|河南省|湖北省|湖南省|广东省|海南省|四川省|贵州省|云南省|陕西省|甘肃省|青海省|台湾省){1}''}',-- 针对province字段随机生成省份，用于后续通过省份统计
      'fields.full_name.expression' = '#{regexify ''(华为智慧屏V65i 65英寸 HEGE-560B 4K全面屏智能电视机 多方视频通话 AI升降摄像头 4GB+32GB 星际黑|Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 冰雾白 游戏智能手机 小米 红米|小米10 至尊纪念版 双模5G 骁龙865 120HZ高刷新率 120倍长焦镜头 120W快充 8GB+128GB 透明版 游戏手机|小米10 至尊纪念版 双模5G 骁龙865 120HZ高刷新率 120倍长焦镜头 120W快充 12GB+256GB 陶瓷黑 游戏手机|Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 冰雾白 游戏智能手机 小米 红米|华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万超感知徕卡三摄 30倍数字变焦 8GB+128GB亮黑色全网通5G手机|Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机|华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万超感知徕卡三摄 30倍数字变焦 6GB+128GB冰霜银全网通5G手机){1}''}',                                               -- 针对full_name字段随机生成产品名，用于后续通过产品名热词拆分
      'fields.click_time.expression' = '#{date.past ''6'',''1'',''SECONDS''}',            -- 针对click_time 字段随机生成比当前时间有1-6秒的延迟的时间数据
      'rows-per-second'          = '50'
      );

DROP TABLE IF EXISTS kafka_page_view;
CREATE TABLE kafka_page_view(
                                product_id       INT,
                                sex              STRING,
                                province         STRING,
                                full_name        STRING,
                                click_time       TIMESTAMP(3),
                                WATERMARK FOR click_time AS click_time - INTERVAL '2' SECOND
) WITH (
      'connector' = 'kafka',                                                                                -- 必选参数, 可选 'kafka','kafka-0.11'. 注意选择对应的内置  Connector
      'topic' = 'page_view_20220120',                                                                       -- 必选参数, 指定kafka topic
      'scan.startup.mode' = 'latest-offset',                                                                -- 非必须参数,默认为group-offset消费组的offset。指定latest-offset为读取topic下最新的数据
      'properties.bootstrap.servers' = '172.16.10.27:9092,172.16.10.28:9092,172.16.10.30:9092',             -- 必选参数, 指定kafka brokers
      'properties.group.id' = 'record',                                                                     -- 必选参数, 指定 Group ID
      'format' = 'json',                                                                                    -- 必选参数, 选择value消息的序列化格式
      'json.ignore-parse-errors' = 'true',                                                                  -- 非必选参数, 忽略 JSON 结构解析异常
      'json.fail-on-missing-field' = 'false'                                                                -- 非必选参数, 如果设置为 true, 则遇到缺失字段会报错 设置为 false 则缺失字段设置为 null
      );

DROP TABLE IF EXISTS clickhouse_key_words;
CREATE TABLE clickhouse_key_words(
                                     province  STRING,
                                     sex       STRING,
                                     word      STRING,
                                     stt       TIMESTAMP(3),
                                     edt       TIMESTAMP(3),
                                     ct        BIGINT
) WITH (
      'connector' = 'clickhouse',
      'url' = 'jdbc:clickhouse://172.16.10.246:8123/pk', -- 可配置集群地址，写入时随机选择连接写入，不会一直使用一个连接写入
      'table-name' = 'clickhouse_key_words',
      'username' = 'default',
      'password' = 'default',
      'format' = 'json'
      );

CREATE FUNCTION sex_trans as 'com.dataomnis.example.UdfDemo';

INSERT INTO kafka_page_view SELECT product_id,sex_trans(sex) as sex,province,full_name,click_time FROM page_view;

INSERT INTO clickhouse_key_words
SELECT province,sex,word,TUMBLE_START(click_time,INTERVAL '30' SECOND) AS stt,TUMBLE_END(click_time,INTERVAL '30' SECOND) AS edt,COUNT(*) ct FROM(
                                                                                                                                                     SELECT CAST(T.word AS STRING) AS word,v.* FROM kafka_page_view AS v,LATERAL TABLE(wordsplit(full_name)) AS T(word)
                                                                                                                                                 ) GROUP BY TUMBLE(click_time,INTERVAL '30' SECOND),province,sex,word;