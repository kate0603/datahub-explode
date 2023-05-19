## **简介**
期望是关于您的数据的断言。

- Great Expectations是一个开源的数据验证和文档框架，用于编写测试以确保数据管道中的数据质量。有助于提高管道中数据的质量，减少错误的风险，从而在决策和业务结果方面取得更好的表现。
- 优势：
   - 易于使用：Great Expectations提供一个易于使用的Python API，可以与现有的数据管道集成。
   -  灵活性：Great Expectations支持多种数据源，包括SQL数据库、Pandas数据帧和Spark。 
   -  自动化：Great Expectations使得数据验证和测试自动化，减少了数据验证的手动工作量。 
   - 文档化：Great Expectations提供了数据文档化功能，包括数据血缘、数据剖析和数据质量报告。 
   - 协作性：Great Expectations使得数据工程师、分析师和利益相关者之间能够合作，确保数据管道得到有效验证和文档化。 

![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1684114794881-34317396-3e59-43b3-b10f-225f4949c9bb.png#averageHue=%23fbf6f3&clientId=ud362defa-eed3-4&from=paste&height=264&id=u7af2989b&originHeight=522&originWidth=1141&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=126113&status=done&style=none&taskId=u7a254c61-007e-4d00-9d4f-460b397fd4d&title=&width=577.4000244140625)
## 架构组件

1.  **Data Context（数据上下文）**：Great Expectations的核心组件，负责维护数据源和验证配置、运行验证步骤、生成验证报告等。 
2.  **Validators（验证器）**：Great Expectations的验证器是实际验证数据的组件。验证器可以基于单个数据源或多个数据源，对数据运行各种针对性的验证规则。 
3.  **Store（存储）**：Great Expectations支持各种存储和元数据存储的机制，比如本地文件系统、对象存储、AWS S3等。 
4.  **Expectations（验证规则）**：验证规则是一组验证逻辑，用于检查数据是否满足预期的标准。Great Expectations支持各种验证规则，包括数据类型、唯一性、区间检查等。 
5.  **Data Assets（数据资产）**：Great Expectations将验证规则附加到数据资产上，比如数据表或文件等。这样，当资产被读取时，所有的验证规则都会被自动执行并生成结果。 
6.  **Reports（报告）**：Great Expectations支持生成各种验证报告，包括HTML、JSON等格式。 

总体来说，Great Expectations的架构使得数据验证和监控变得更加易于处理，减轻了数据工程师和数据分析师的工作负担，提高了数据的质量和准确性。
## 流程

1. 设置 初始化数据上下文
2. 连接到数据库
3. 创建expectations
4. 验证数据
## 期望函数
### 行
| 函数 | 功能 | 示例 | 备注 |
| --- | --- | --- | --- |
| [expect_value_at_index](https://greatexpectations.io/expectations/expect_value_at_index) | 在列的每个元素内的给定索引位置检查指定的值 | expect_value_at_index(**{'column': 'mostly_has_decimal', 'value': '.', 'index': -3, 'mostly': 0.6}) |  |
| [expect_table_row_count_to_equal_other_table](https://greatexpectations.io/expectations/expect_table_row_count_to_equal_other_table) | 期望行数等于另一个表中的行数 |  |  |
| [expect_table_row_count_to_equal](https://greatexpectations.io/expectations/expect_table_row_count_to_equal) | 期望行数等于一个值 |  |  |
| [expect_table_row_count_to_be_between](https://greatexpectations.io/expectations/expect_table_row_count_to_be_between) | 期望行数介于两个值之间 |  |  |
| [expect_column_values_to_follow_rule](https://greatexpectations.io/expectations/expect_column_values_to_follow_rule) | 此期望将列的所有行与给定的输入表达式
进行比较 |  |  |
| [expect_column_values_to_change_between](https://greatexpectations.io/expectations/expect_column_values_to_change_between) | 给定一个数值列表，
检查当前行与前一行之间的差异
在预期的差值范围内 |  |  |

### 单列
| 函数 | 功能 | 示例 | 备注 |
| --- | --- | --- | --- |
| [expect_column_distinct_values_to_equal_set](https://greatexpectations.io/expectations/expect_column_distinct_values_to_equal_set) | 期望一组不同的列值等于给定的集合中 |  |  |
| [expect_column_distinct_values_to_contain_set](https://greatexpectations.io/expectations/expect_column_distinct_values_to_contain_set) | 期望一组不同的列值包含了给定的集合中 |  |  |
| [expect_column_distinct_values_to_be_in_set](https://greatexpectations.io/expectations/expect_column_distinct_values_to_be_in_set) | 期望一组不同的列值包含在给定的集合中 |  |  |
| [expect_table_columns_to_match_set](https://greatexpectations.io/expectations/expect_table_columns_to_match_set) | 期望列与 *unordered* 集完全匹配 |  |  |
| [expect_table_columns_to_match_ordered_list](https://greatexpectations.io/expectations/expect_table_columns_to_match_ordered_list) | 期望列与指定的列表完全匹配 |  |  |
| [expect_table_column_count_to_equal](https://greatexpectations.io/expectations/expect_table_column_count_to_equal) | 期望列数等于一个值 |  |  |
| [expect_table_column_count_to_be_between](https://greatexpectations.io/expectations/expect_table_column_count_to_be_between) | 期望列数介于两个值之间 |  |  |
| [expect_select_column_values_to_be_unique_within_record](https://greatexpectations.io/expectations/expect_select_column_values_to_be_unique_within_record) | 期望每个记录的值在列出的列中是唯一的 |  |  |
| [expect_multicolumn_sum_to_equal](https://greatexpectations.io/expectations/expect_multicolumn_sum_to_equal) | 期望行的和 与 每行相同，只在指定列求和，等于特定值 |  |  |
| [expect_foreign_keys_in_column_a_to_exist_in_column_b](https://greatexpectations.io/expectations/expect_foreign_keys_in_column_a_to_exist_in_column_b) | 确保感兴趣的列 (ColumnA) 中的值 在一个valueset中提供的dataframe（df 参数）+ 列（column_B 参数）或作为 pandas.DataFrame() 支持的元素列表（例如字典列表 [{" col_name": value},], list of tuples [(value, value), (value, value)]。这是一个非常实验性的实现来描述功能，但是一旦跨表期望模板可用，就应该重新审视这个期望. |  |  |
| [expect_column_values_to_not_be_null](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null) | 期望列值不为空 |  |  |
| [expect_column_values_to_be_unique](https://greatexpectations.io/expectations/expect_column_values_to_be_unique) | 期望每列值都是唯一的 |  |  |
| [expect_column_values_to_be_string_integers_increasing](https://greatexpectations.io/expectations/expect_column_values_to_be_string_integers_increasing) | 希望列中包含的字符串类型的整数是递增的 |  |  |
| [expect_column_values_to_be_null](https://greatexpectations.io/expectations/expect_column_values_to_be_null) | 期望列值为空 |  |  |
| [expect_column_values_to_be_of_type](https://greatexpectations.io/expectations/expect_column_values_to_be_of_type) | 期望一列包含指定数据类型的值 |  |  |
| [expect_column_values_to_be_in_set](https://greatexpectations.io/expectations/expect_column_values_to_be_in_set) | 期望每个列的值都在给定的集合中 |  |  |
| [expect_column_values_to_be_increasing](https://greatexpectations.io/expectations/expect_column_values_to_be_increasing) | 列值会增加 |  |  |
| [expect_column_values_to_be_decreasing](https://greatexpectations.io/expectations/expect_column_values_to_be_decreasing) | 列值会减少 |  |  |
| [expect_column_unique_value_count_to_be_between](https://greatexpectations.io/expectations/expect_column_unique_value_count_to_be_between) | 期望唯一值的数量介于最小值和最大值之间 |  |  |
| [expect_column_to_exist](https://greatexpectations.io/expectations/expect_column_to_exist) | 期望指定的列存在 |  |  |
| [expect_column_sum_to_be_between](https://greatexpectations.io/expectations/expect_column_sum_to_be_between) | 期望列总和介于最小值和最大值之间 |  |  |
| [expect_column_sum_to_be](https://greatexpectations.io/expectations/expect_column_sum_to_be) | 期望一列的总和恰好是一个值 |  |  |
| [expect_column_proportion_of_unique_values_to_be_between](https://greatexpectations.io/expectations/expect_column_proportion_of_unique_values_to_be_between) | 期望唯一值的比例介于最小值和最大值之间 |  |  |
| [expect_column_most_common_value_to_be_in_set](https://greatexpectations.io/expectations/expect_column_most_common_value_to_be_in_set) | 期望最常见的值在指定值集中 |  |  |
| [expect_column_min_to_be_between](https://greatexpectations.io/expectations/expect_column_min_to_be_between) | 期望列最小值介于最小值和最大值之间 |  |  |
| [expect_column_median_to_be_between](https://greatexpectations.io/expectations/expect_column_median_to_be_between) | 期望列中位数介于最小值和最大值之间 |  |  |
| [expect_column_mean_to_be_between](https://greatexpectations.io/expectations/expect_column_mean_to_be_between) | 期望列均值介于最小值和最大值（含）之间 |  |  |
| [expect_column_max_to_be_between](https://greatexpectations.io/expectations/expect_column_max_to_be_between) | 期望列最大值介于最小值和最大值之间 |  |  |
|  |  |  |  |

### 列条目
| 函数 | 功能 | 示例 | 备注 |
| --- | --- | --- | --- |
| [expect_column_values_to_be_between](https://greatexpectations.io/expectations/expect_column_values_to_be_between) | 期望列条目介于最小值和最大值（包括）之间 |  |  |
| [expect_column_value_lengths_to_equal](https://greatexpectations.io/expectations/expect_column_value_lengths_to_equal) | 期望列条目是长度等于提供值的字符串 |  |  |
| [expect_column_value_lengths_to_be_between](https://greatexpectations.io/expectations/expect_column_value_lengths_to_be_between) | 期望列条目是长度在最小值和最大值（包括）之间的字符串 |  |  |
| [expect_column_values_to_not_be_in_set](https://greatexpectations.io/expectations/expect_column_values_to_not_be_in_set) | 期望列条目不在集合中 |  |  |
| [expect_column_values_to_not_contain_special_characters](https://greatexpectations.io/expectations/expect_column_values_to_not_contain_special_characters) | 期望列条目不包含特殊字符 |  |  |
| [expect_column_values_to_not_contain_character](https://greatexpectations.io/expectations/expect_column_values_to_not_contain_character) | 期望列值集不包含给定字符 |  |  |
| [expect_column_values_to_not_match_like_pattern_list](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern_list) | 期望列条目是不匹配任何提供的类似模式表达式列表的字符串 |  |  |
| [expect_column_values_to_not_match_like_pattern](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern) | 期望列条目是与给定的类似模式表达式不匹配的字符串 |  |  |
| [expect_column_values_to_match_strftime_format](https://greatexpectations.io/expectations/expect_column_values_to_match_strftime_format) | 期望列条目是表示具有给定格式的日期或时间的字符串 |  |  |
| [expect_column_values_to_match_json_schema](https://greatexpectations.io/expectations/expect_column_values_to_match_json_schema) | 期望列条目是与给定 JSON 模式匹配的 JSON 对象 |  |  |

### 正则
| 函数 | 功能 | 示例 | 备注 |
| --- | --- | --- | --- |
| [expect_column_values_to_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_match_regex_list) | 期望列条目是可以与正则表达式列表中的任何一个或全部匹配的字符串 |  |  |
| [expect_column_values_to_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_match_regex) | 期望列条目是与给定正则表达式匹配的字符串 |  |  |
| [expect_column_values_to_match_like_pattern_list](https://greatexpectations.io/expectations/expect_column_values_to_match_like_pattern_list) | 期望列条目是与给定正则表达式列表相似的字符串 |  |  |
| [expect_column_values_to_match_like_pattern](https://greatexpectations.io/expectations/expect_column_values_to_match_like_pattern) | 期望列条目是与给定正则表达式相似的字符串 |  |  |
| [expect_column_values_to_not_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex_list) | 期望列条目是不匹配任何正则表达式列表的字符串 |  |  |
| [expect_column_values_to_not_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex) | 期望列条目是不匹配给定正则表达式的字符串。
正则表达式不得与提供的字符串的任何部分匹配。
例如，“[at]+”将按预期识别以下字符串：“fish”、“dog”，并将以下字符串识别为意外：“cat”、“hat” |  |  |

### 多列
| 函数 | 功能 | 示例 | 备注 |
| --- | --- | --- | --- |
| [expect_compound_columns_to_be_unique](https://greatexpectations.io/expectations/expect_compound_columns_to_be_unique) | 期望复合列是唯一的  |  |  |
| [expect_column_pair_values_to_be_in_set](https://greatexpectations.io/expectations/expect_column_pair_values_to_be_in_set) | 期望来自列A和B的成对值属于一组有效的对 |  |  |
| [expect_column_pair_values_to_be_equal](https://greatexpectations.io/expectations/expect_column_pair_values_to_be_equal) | 期望A列中的值与B列中的值相同。 |  |  |
| [expect_column_pair_values_a_to_be_greater_than_b](https://greatexpectations.io/expectations/expect_column_pair_values_a_to_be_greater_than_b) | 期望A列中的值大于B列。 |  |  |

### 数学函数
| 函数 | 功能 | 示例 | 备注 |
| --- | --- | --- | --- |
| [expect_table_linear_feature_importances_to_be](https://greatexpectations.io/expectations/expect_table_linear_feature_importances_to_be) | 期望线性回归表中指定列的特征重要性达到阈值 |  |  |
| [expect_column_values_to_be_polygon_area_between](https://greatexpectations.io/expectations/expect_column_values_to_be_polygon_area_between) | 这个期望将计算每个多边形/多多边形的面积，以平方公里为单位，并检查它是否在两个值之间 |  |  |
| [expect_column_values_to_be_fibonacci_number](https://greatexpectations.io/expectations/expect_column_values_to_be_fibonacci_number) | 期望值是斐波那契数 |  |  |
| [expect_column_kurtosis_to_be_between](https://greatexpectations.io/expectations/expect_column_kurtosis_to_be_between) | 期望列峰度介于两者之间 |  |  |
| [expect_column_wasserstein_distance_to_be_less_than](https://greatexpectations.io/expectations/expect_column_wasserstein_distance_to_be_less_than) | 列的分布相似程度（EMD）小于期待值 |  |  |
| [expect_column_values_to_be_normally_distributed](https://greatexpectations.io/expectations/expect_column_values_to_be_normally_distributed) | 测试列值是否正态分布 |  |  |
| [expect_column_values_to_not_be_outliers](https://greatexpectations.io/expectations/expect_column_values_to_not_be_outliers) | 期望列值不是离群值。用户被要求指定列、方法和乘数。目前
支持标准偏差(std)和分位数范围(iqr)。 |  |  |
| [expect_column_value_z_scores_to_be_less_than](https://greatexpectations.io/expectations/expect_column_value_z_scores_to_be_less_than) | 期望列值的 归一化（z-scores） 小于给定的阈值 |  |  |
| [expect_column_stdev_to_be_between](https://greatexpectations.io/expectations/expect_column_stdev_to_be_between) | 期望列标准偏差介于最小值和最大值之间 |  |  |
| [expect_column_skew_to_be_between](https://greatexpectations.io/expectations/expect_column_skew_to_be_between) | 列偏斜介于两者之间 |  |  |
| [expect_column_quantile_values_to_be_between](https://greatexpectations.io/expectations/expect_column_quantile_values_to_be_between) | 期望特定提供的列分位数介于提供的最小值和最大值之间 |  |  |
| [expect_column_kl_divergence_to_be_less_than](https://greatexpectations.io/expectations/expect_column_kl_divergence_to_be_less_than) | 期望指定列相对于分区对象的 Kulback-Leibler (KL) 散度（相对熵）低于提供的阈值 |  |  |
| [expect_column_distribution_to_match_benfords_law](https://greatexpectations.io/expectations/expect_column_distribution_to_match_benfords_law) | 测试数据是否符合本福德法律欺诈检测算法。
使用80@ p-value的卡方拟合优度检验 |  |  |
| [expect_column_discrete_entropy_to_be_between](https://greatexpectations.io/expectations/expect_column_discrete_entropy_to_be_between) | 期望列离散熵介于最小值和最大值之间。 |  |  |
| [expect_column_values_confidence_for_data_label_to_be_greater_than_or_equal_to_threshold](https://greatexpectations.io/expectations/expect_column_values_confidence_for_data_label_to_be_greater_than_or_equal_to_threshold) | 此功能建立在 Great Expectations 的自定义柱状图预期之上。
该函数针对用户指定列中的每一行询问一个是/否问题；
即DataProfiler模型提供的置信度阈值是否超过用户指定的阈值。 |  |  |

### 地理位置
| 函数 | 功能 | 示例 | 备注 |
| --- | --- | --- | --- |
| [expect_column_values_to_be_valid_ipv6](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_ipv6) | 此期望验证数据是否符合有效的 IPv6 地址格式。 |  |  |
| [expect_column_values_to_be_valid_ipv4](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_ipv4) | 此期望验证数据是否符合有效的 IPv4 地址格式。 |  |  |
| [expect_column_values_to_be_private_ipv4_class](https://greatexpectations.io/expectations/expect_column_values_to_be_private_ipv4_class) | 期望在传入参数的 IP 类（A、B、C）中提供的私有 IP v4 地址 |  |  |
| [expect_column_values_to_be_private_ip_v6](https://greatexpectations.io/expectations/expect_column_values_to_be_private_ip_v6) | 期望值为私有 IP v6 地址 |  |  |
| [expect_column_values_to_be_private_ip_v4](https://greatexpectations.io/expectations/expect_column_values_to_be_private_ip_v4) | 期望值为私有 IP 地址 |  |  |
| [expect_column_values_to_be_lat_lon_coordinates_in_range_of_given_point](https://greatexpectations.io/expectations/expect_column_values_to_be_lat_lon_coordinates_in_range_of_given_point) | 期望列中的值是给定度数小数（纬度，经度）点的指定范围内的度数（纬度，经度）的元组 |  |  |
| [expect_column_values_to_be_daytime](https://greatexpectations.io/expectations/expect_column_values_to_be_daytime) | 期望提供的时间戳是给定 GPS 坐标（纬度、经度）的白天 |  |  |
| [expect_column_values_ip_asn_country_code_in_set](https://greatexpectations.io/expectations/expect_column_values_ip_asn_country_code_in_set) | 期望在集合中提供 IP 地址 ASN 国家代码 |  |  |
| [expect_column_values_ip_address_in_network](https://greatexpectations.io/expectations/expect_column_values_ip_address_in_network) | 期望在传入参数的网络中提供的 IP 地址 |  |  |
| [expect_column_values_geometry_to_be_within_place](https://greatexpectations.io/expectations/expect_column_values_geometry_to_be_within_place) | 期望列值作为几何图形位于可以通过地理编码返回的位置（作为形状） |  |  |
| [expect_column_values_geometry_distance_to_address_to_be_between](https://greatexpectations.io/expectations/expect_column_values_geometry_distance_to_address_to_be_between) | 期望作为几何点的列值在距地理编码对象一定距离之间。 |  |  |
| [expect_column_average_to_be_within_range_of_given_point](https://greatexpectations.io/expectations/expect_column_average_to_be_within_range_of_given_point) | 期望一列十进制度数、纬度/经度坐标的平均值在给定点的范围内。 |  |  |
| [expect_column_values_to_be_us_zipcode_within_mile_radius_of_given_zipcode](https://greatexpectations.io/expectations/expect_column_values_to_be_us_zipcode_within_mile_radius_of_given_zipcode) | 给定一个邮政编码和一个半径，该期望检查表中一列中的所有邮政编码是否在指定的范围内
给定邮编的半径(以英里为单位)。 |  |  |
| [expect_column_values_point_within_geo_region](https://greatexpectations.io/expectations/expect_column_values_point_within_geo_region) | 该期望将检查(经度、纬度)元组，以查看它是否位于由
用户。为了进行地理计算，它利用了Geopandas库。所以目前它只支持这些国家
都在世界数据库中。重要的是，国家是由它们的iso_a3国家代码定义的，而不是它们的
全名 |  |  |
| [expect_column_average_lat_lon_pairwise_distance_to_be_less_than](https://greatexpectations.io/expectations/expect_column_average_lat_lon_pairwise_distance_to_be_less_than) | 该期望将计算每个（纬度，经度）对之间的成对harsine距离 |  |  |

### 有效值
| 函数 | 功能 | 示例 | 备注 |
| --- | --- | --- | --- |
| [expect_column_values_url_has_got_valid_cert](https://greatexpectations.io/expectations/expect_column_values_url_has_got_valid_cert) | 期望提供的 url 已获得有效证书 |  |  |
| [expect_column_values_to_contain_valid_email](https://greatexpectations.io/expectations/expect_column_values_to_contain_valid_email) | 列包含有效邮件 |  |  |
| [expect_column_values_to_be_valid_uuid](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_uuid) | 此期望验证数据是否符合有效的 UUID 格式 |  |  |
| [expect_column_values_to_be_valid_us_state_or_territory](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_us_state_or_territory) | 期望此列中的值是有效的州缩写 |  |  |
| [expect_column_values_to_be_valid_us_state_or_territory_abbreviation](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_us_state_or_territory_abbreviation) | 此列中的值应为有效的州或领地缩写 |  |  |
| [expect_column_values_to_be_valid_us_state_abbreviation](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_us_state_abbreviation) | 期望此列中的值是有效的州缩写 |  |  |
| [expect_column_values_to_be_valid_us_state](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_us_state) | 期望此列中的值是有效的州缩写 |  |  |
| [expect_column_values_to_be_valid_udp_port](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_udp_port) | 此期望验证数据是否符合有效的 UDP 端口号 |  |  |
| [expect_column_values_to_be_valid_tcp_port](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_tcp_port) | 此期望验证数据是否符合有效的 TCP 端口号 |  |  |
| [expect_column_values_to_be_valid_ssn](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_ssn) | 此期望验证数据是否符合有效的社会安全号码格式 |  |  |
| [expect_column_values_to_be_valid_price](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_price) | 此期望验证数据是否符合有效的价格格式 |  |  |
| [expect_column_values_to_be_valid_phonenumber](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_phonenumber) | 此期望验证数据是否符合有效的电话号码格式。 |  |  |
| [expect_column_values_to_be_valid_mime](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_mime) | 此期望验证数据是否符合有效的 MIME 类型格式 |  |  |
| [expect_column_values_to_be_valid_mac](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_mac) | 此期望验证数据是否符合有效的 MAC 地址格式 |  |  |
| [expect_column_values_to_be_valid_iso_country](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_iso_country) | 该期望根据 ISO 3166 验证数据是否符合有效的国家/地区代码 |  |  |
| [expect_column_values_to_be_valid_iban](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_iban) | 此期望验证数据是否符合有效的 IBAN 格式 |  |  |
| [expect_column_values_to_be_valid_iana_timezone](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_iana_timezone) | 此列中的值应为有效的 IANA 时区字符串 |  |  |
| [expect_column_values_to_be_valid_http_status_name](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_http_status_name) | 此期望验证数据是否符合有效的 HTTP 状态名称 |  |  |
| [expect_column_values_to_be_valid_http_status_code](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_http_status_code) | 此期望验证数据是否符合有效的 HTTP 状态代码 |  |  |
| [expect_column_values_to_be_valid_hex_color](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_hex_color) | 此期望验证数据是否符合有效的十六进制颜色代码格式。 |  |  |
| [expect_column_values_to_be_slug](https://greatexpectations.io/expectations/expect_column_values_to_be_slug) | 期望值是有效的 slug |  |  |
| [expect_column_values_to_be_iso_languages](https://greatexpectations.io/expectations/expect_column_values_to_be_iso_languages) | 期望值是有效的 ISO 639-3 语言 |  |  |
| [expect_column_values_to_be_valid_urls](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_urls) | 期望该列是有效的 url。
将行值映射到正则表达式以检查值是否为有效 url |  |  |

### 其他
| 函数 | 功能 | 示例 | 备注 |
| --- | --- | --- | --- |
| [expect_column_values_url_hostname_match_with_cert](https://greatexpectations.io/expectations/expect_column_values_url_hostname_match_with_cert) | 期望提供的 url 的主机名与证书匹配 |  |  |
| [expect_column_values_to_be_weekday](https://greatexpectations.io/expectations/expect_column_values_to_be_weekday) | 期望值为工作日 |  |  |
| [expect_column_values_to_be_vectors](https://greatexpectations.io/expectations/expect_column_values_to_be_vectors) | 期望列值是向量 |  |  |
| [expect_column_values_to_match_xml_schema](https://greatexpectations.io/expectations/expect_column_values_to_match_xml_schema) | 期望列条目是匹配给定 [XMLSchema]([https://en.wikipedia.org/wiki/XML_schema)](https://en.wikipedia.org/wiki/XML_schema)) 的 XML 文档 |  |  |
| [expect_column_values_to_be_xml_parseable](https://greatexpectations.io/expectations/expect_column_values_to_be_xml_parseable) | 期望列条目是用 XML 编写的数据 |  |  |
| [expect_column_values_to_be_valid_wikipedia_articles](https://greatexpectations.io/expectations/expect_column_values_to_be_valid_wikipedia_articles) | 此期望检查列是否包含 Wikipedia 文章的有效标题/标题 |  |  |
| [expect_column_values_to_be_secure_passwords](https://greatexpectations.io/expectations/expect_column_values_to_be_secure_passwords) | 期望列条目是安全密码，由期望参数定义 |  |  |
| [expect_column_values_to_be_json_parseable](https://greatexpectations.io/expectations/expect_column_values_to_be_json_parseable) | 期望列条目是用 JavaScript 对象表示法编写的数据 |  |  |
| [expect_column_values_to_be_edtf_parseable](https://greatexpectations.io/expectations/expect_column_values_to_be_edtf_parseable) | 期望列条目可以使用 [扩展日期/时间格式 (EDTF) 规范] |  |  |
| [expect_column_values_are_in_language](https://greatexpectations.io/expectations/expect_column_values_are_in_language) | 期望该列使用指定的语言 |  |  |
| [expect_column_values_to_be_dateutil_parseable](https://greatexpectations.io/expectations/expect_column_values_to_be_dateutil_parseable) | 期望列条目可以使用 dateutil 解析 |  |  |
| [expect_column_values_to_be_ascii](https://greatexpectations.io/expectations/expect_column_values_to_be_ascii) | 期望列值集是 ASCII 字符 |  |  |
| [expect_column_values_to_be_alphabetical](https://greatexpectations.io/expectations/expect_column_values_to_be_alphabetical) | 给定一个字符串值列表，检查该列表是否按字母顺序排列(向前或向后)(用
“反向”参数)。比较是不区分大小写的。使用“mostly”可以告诉你有多少项是按字母顺序排列的
相对于列表中紧接的前一项 |  |  |
| [expect_column_values_to_be_a_non_bot_user_agent](https://greatexpectations.io/expectations/expect_column_values_to_be_a_non_bot_user_agent) | 期望用户代理不是机器人 |  |  |
| [expect_column_values_number_of_decimal_places_to_equal](https://greatexpectations.io/expectations/expect_column_values_number_of_decimal_places_to_equal) | 此期望测试一列中的所有值的小数点位数是否与
输入的小数位数。在小数点都是0(整数)的情况下，
该值自动传递。目前还没有弄清楚如何将十进制的0转换为字符串 |  |  |

## 示例
### 示例-spark读数据，定制化期望
```python
def test1():
    from ruamel import yaml
    from big_data_public.spark.spark_context import spark
    import great_expectations as ge
    from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest, Batch
    from great_expectations.data_context import BaseDataContext
    from great_expectations.data_context.types.base import (
        DataContextConfig,
        InMemoryStoreBackendDefaults,
    )
    from great_expectations.profile.user_configurable_profiler import (
        UserConfigurableProfiler,
    )
    from great_expectations.render.renderer import ValidationResultsPageRenderer
    from great_expectations.render.view import DefaultJinjaPageView

    df = spark.read.json("obs:/xx/dt=2022-02-14/")

    store_backend_defaults = InMemoryStoreBackendDefaults()
    data_context_config = DataContextConfig(
        store_backend_defaults=store_backend_defaults,
        checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
    )
    context = BaseDataContext(project_config=data_context_config)

    datasource_config = {
        "name": "my_spark_dataframe",
        "class_name": "Datasource",
        "execution_engine": {"class_name": "SparkDFExecutionEngine"}, # PandasExecutionEngine
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            }
        },
    }

    context.test_yaml_config(yaml.dump(datasource_config))

    context.add_datasource(**datasource_config)

    # Here is a RuntimeBatchRequest using a dataframe
    batch_request = RuntimeBatchRequest(
        datasource_name="my_spark_dataframe",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="test",  # This can be anything that identifies this data_asset for you
        batch_identifiers={"batch_id": "default_identifier"},
        runtime_parameters={"batch_data": df},  # Your dataframe goes here
    )

    context.create_expectation_suite(
        expectation_suite_name="test_suite", overwrite_existing=True
    )
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )

    # all_columns = [i for i in df.columns]
    # print(all_columns)
    # suite_columns = ['app_key', 'device_id', 'role_id', 'logical_region_id', 'cp_order_id', 'cp_point']

    # ignored_columns = [i for i in all_columns if i not in suite_columns]
    # print(ignored_columns)

    # profiler = UserConfigurableProfiler(
    #    profile_dataset=validator,
    #    excluded_expectations=None,
    #    ignored_columns=ignored_columns,
    #    not_null_only=True,
    #    primary_or_compound_key=None,
    ##    semantic_types_dict=None,
    #   table_expectations_only=False,
    #   value_set_threshold="NONE",
    # )
    # suite = profiler.build_suite()
    data = validator.validate()
    # print(suite.expectation_suite_name)
    # validator
    # data = validator.get_expectation_suite(discard_failed_expectations=False)

    print(data)
    # columns = ['cp_point']
    columns = ['app_key', 'device_id', 'role_id', 'logical_region_id', 'cp_order_id', 'cp_point']
    methods = [
        # "expect_column_values_to_be_unique",
        "expect_column_values_to_not_be_null",
        # "expect_column_distinct_values_to_be_in_set",
    ]
    number_methods = [
        "expect_column_max_to_be_between",
        "expect_column_mean_to_be_between",
        "expect_column_median_to_be_between",
        "expect_column_min_to_be_between",
    ]
    columns_type = dict(df.dtypes)
    print(columns_type)
    # print(dir(validator))

    for column in columns:
        if column == 'cp_order_id':
            result = getattr(validator, 'expect_column_values_to_be_unique')(column).result
            print(column, 'expect_column_values_to_be_unique', result)
        for method in methods:
            result = getattr(validator, method)(column).result
            print(column, method, result)
        if columns_type[column] in ['bigint', 'double']:
            for method in number_methods:
                result = getattr(validator, method)(column).result
                print(column, method, result)
    # 在维度表中
    result = validator.expect_column_pair_values_to_be_in_set("logical_region_id","server_id", [(1010,1),(1012,1)]).result
    # 空值
    result = validator.expect_column_values_to_not_be_null("device_id").result
    # 值类型
    # result = validator.expect_column_values_to_be_of_type("pay_type_id", "StringType").result
    # 满足正则
    result = validator.expect_column_values_to_match_regex_list("ip", ["^(2(5[0-5]{1}|[0-4]\d{1})|[0-1]?\d{1,2})(\.(2(5[0-5]{1}|[0-4]\d{1})|[0-1]?\d{1,2})){3}$"]).result
    # 值不在列表中
    result = validator.expect_column_values_to_not_be_in_set("sp_currency_code", ['CNY']).result
    # 值在列表中
    result = validator.expect_column_values_to_be_in_set("sp_currency_code", ['CNY']).result
    # 唯一值
    result = validator.expect_column_values_to_be_unique("cp_order_id").result
    # 最大/最小/平均/标准差
    result = validator.expect_column_max_to_be_between("amount").result
    result = validator.expect_column_min_to_be_between("amount").result
    result = validator.expect_column_median_to_be_between("amount").result
    result = validator.expect_column_mean_to_be_between("amount").result # 需转为double类型
    # 转换成html
    # document_model = ValidationResultsPageRenderer(data_context=context).render(data)
    # html_code = DefaultJinjaPageView().render(document_model)
    # print(html_code)
```
### 示例-连接postgresql
```python
from ruamel import yaml
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import RuntimeBatchRequest


CONNECTION_STRING = "postgresql+psycopg2://user:password@ip:port/database"

store_backend_defaults = InMemoryStoreBackendDefaults()
data_context_config = DataContextConfig(
    store_backend_defaults=store_backend_defaults,
    checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
)
context = BaseDataContext(project_config=data_context_config)

datasource_config = {
    "name": "my_postgres_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "include_schema_name": True,
        },
    },
}

datasource_config["execution_engine"]["connection_string"] = CONNECTION_STRING
context.test_yaml_config(yaml.dump(datasource_config))
context.add_datasource(**datasource_config)

# Here is a RuntimeBatchRequest using a query
batch_request = RuntimeBatchRequest(
    datasource_name="my_postgres_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from public.dim_game_info_1 LIMIT 10"},
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())
```
### 示例-自定义列聚合期望
```python
# -*- coding: utf-8 -*-
"""
Example of custom expectation with renderer.
This custom expectation can be run as part of a checkpoint with the script run_checkpoint_with_custom_expectation.py
in the getting_started_tutorial_final_v3_api directory, e.g.
getting_started_tutorial_final_v3_api$ python run_checkpoint_with_custom_expectation.py
See corresponding documentation:
* https://docs.greatexpectations.io/en/latest/guides/how_to_guides/creating_and_editing_expectations/how_to_create_custom_expectations.html
* https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_docs/how_to_create_renderers_for_custom_expectations.html
"""

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.metrics import (
    ColumnMetricProvider,
    column_aggregate_value,
    column_aggregate_partial,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import (
    RenderedStringTemplateContent,
    RenderedTableContent,
    RenderedBulletListContent,
    RenderedGraphContent,
)
from great_expectations.render.util import substitute_none_for_missing

from typing import Any, Dict, List, Optional, Union

from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
)


class ColumnCustomMax(ColumnMetricProvider):
    """MetricProvider Class for Custom Aggregate Max MetricProvider"""

    metric_name = "column.aggregate.custom.max"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Max Implementation"""
        return column.max()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        """SqlAlchemy Max Implementation"""
        return sa.func.max(column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, _table, _column_name, **kwargs):
        """Spark Max Implementation"""
        types = dict(_table.dtypes)
        return F.maxcolumn()


class ExpectColumnMaxToBeBetweenCustom(ColumnExpectation):
    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
    metric_dependencies = ("column.aggregate.custom.max",)
    success_keys = ("min_value", "strict_min", "max_value", "strict_max")

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "min_value": None,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "mostly": 1,
    }
    args_keys = (
        "column",
        "min_value",
        "strict_min"
        "max_value",
        "strict_max"
    )

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates the given data against the set minimum and maximum value thresholds for the column max"""
        column_max = metrics["column.aggregate.custom.max"]

        # Obtaining components needed for validation
        min_value = self.get_success_kwargs(configuration).get("min_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")

        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = column_max > min_value
            else:
                above_min = column_max >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = column_max < max_value
            else:
                below_max = column_max <= max_value
        else:
            below_max = True

        success = above_min and below_max

        return {"success": success, "result": {"observed_value": column_max}}

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.
        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """
        min_val = None
        max_val = None

        # Setting up a configuration
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        # Ensuring basic configuration parameters are properly set
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column map expectations"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        # Validating that Minimum and Maximum values are of the proper format and type
        if "min_value" in configuration.kwargs:
            min_val = configuration.kwargs["min_value"]

        if "max_value" in configuration.kwargs:
            max_val = configuration.kwargs["max_value"]

        try:
            # Ensuring Proper interval has been provided
            assert (
                min_val is not None or max_val is not None
            ), "min_value and max_value cannot both be none"
            assert min_val is None or isinstance(
                min_val, (float, int)
            ), "Provided min threshold must be a number"
            assert max_val is None or isinstance(
                max_val, (float, int)
            ), "Provided max threshold must be a number"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: ExpectationConfiguration = None,
        result: ExpectationValidationResult = None,
        language: str = None,
        runtime_configuration: dict = None,
        **kwargs,
    ) -> List[
        Union[
            dict,
            str,
            RenderedStringTemplateContent,
            RenderedTableContent,
            RenderedBulletListContent,
            RenderedGraphContent,
            Any,
        ]
    ]:
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        # get params dict with all expected kwargs
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        # build string template
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "values may have any length."
        else:
            at_least_str = (
                "greater than"
                if params.get("strict_min") is True
                else "greater than or equal to"
            )
            at_most_str = (
                "less than"
                if params.get("strict_max") is True
                else "less than or equal to"
            )

            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )

                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = f"values must be {at_least_str} $min_value and {at_most_str} $max_value characters long, at least $mostly_pct % of the time."

                elif params["min_value"] is None:
                    template_str = f"values must be {at_most_str} $max_value characters long, at least $mostly_pct % of the time."

                elif params["max_value"] is None:
                    template_str = f"values must be {at_least_str} $min_value characters long, at least $mostly_pct % of the time."
            else:
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = f"values must always be {at_least_str} $min_value and {at_most_str} $max_value characters long."

                elif params["min_value"] is None:
                    template_str = f"values must always be {at_most_str} $max_value characters long."

                elif params["max_value"] is None:
                    template_str = f"values must always be {at_least_str} $min_value characters long."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        # return simple string
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]
        
# 调用
# from custom_expectation import ExpectColumnMaxToBeBetweenCustom
# r = validator.expect_column_max_to_be_between_custom("passenger_count", min_value=0, max_value=5)

```
### 示例-期望函数过滤条件
目前支持pandas，[文档](https://docs.greatexpectations.io/docs/reference/expectations/conditional_expectations)
```python
	import great_expectations as ge

    my_df = ge.read_csv("../../data/yellow_tripdata_sample_2019-02.csv")
    result = my_df.expect_column_values_to_be_in_set(
        column="vendor_id",
        value_set=[1],
        row_condition="rate_code_id==1",
        condition_parser="pandas",
    )
    print(result)
```
### 示例-checkpoint
![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1677658989375-ce0bf2ac-41ab-4d13-bd4e-bd9c39a0a3e7.png#averageHue=%23e5b85c&clientId=ua08c75a5-2170-4&from=paste&height=261&id=u7624c609&originHeight=1102&originWidth=1871&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=186450&status=done&style=none&taskId=ua882922b-7d22-4cfd-90a6-ce87cf5c888&title=&width=443.4000244140625)
```python
import os
import copy
import pandas as pd
from ruamel import yaml
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
    DataContextConfigDefaults,
)
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)


class ExampleCheckPoint(object):
    def __init__(self):
        # 数据源名称
        self.datasource_name: str = "dwd"
        self.runtime_connector_name: str = "default_runtime_data_connector_name"
        self.expectation_suite_name: str = "suite_test"
        self.inferred_connector_name: str = "inferred_connector_test"
        self.template_file: str = "template_test"

    def data_source(self):
        """
        数据源
        :return:
        """
        # 数据上下文
        context_root_dir = os.getcwd()
        store_backend_defaults = InMemoryStoreBackendDefaults()
        store_backend_defaults.data_docs_sites = copy.deepcopy(
            DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITES.value
        )
        data_context_config = DataContextConfig(
            store_backend_defaults=store_backend_defaults,
            checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
        )
        self.context = BaseDataContext(
            project_config=data_context_config, context_root_dir=context_root_dir
        )

        source_config = {
            "name": self.datasource_name,
            "class_name": "Datasource",
            "execution_engine": {"class_name": "PandasExecutionEngine"},
            "data_connectors": {
                self.inferred_connector_name: {
                    "class_name": "InferredAssetFilesystemDataConnector",
                    "base_directory": "..\data",
                    "default_regex": {
                        "group_names": ["data_asset_name"],
                        "pattern": "(.*)",
                    },
                },
                self.runtime_connector_name: {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
            },
        }
        self.context.test_yaml_config(yaml.dump(source_config))
        self.context.add_datasource(**source_config)

    def get_data_1(self):
        data = pd.read_csv("..\..\data\yellow_tripdata_sample_2019-02.csv")
        return data

    def suite(self):
        data = self.get_data_1()
        batch_request = {
            "datasource_name": self.datasource_name,
            "data_connector_name": self.runtime_connector_name,
            "data_asset_name": f"{self.template_file}",
            "batch_identifiers": {"default_identifier_name": "default_identifier"},
            "runtime_parameters": {"batch_data": data},
        }

        self.context.create_expectation_suite(
            expectation_suite_name=self.expectation_suite_name, overwrite_existing=True
        )

        validator = self.context.get_validator(
            # batch_request=BatchRequest(**batch_request),
            batch_request=RuntimeBatchRequest(**batch_request),
            expectation_suite_name=self.expectation_suite_name,
        )

        ignored_columns = ["log_date"]

        profiler = UserConfigurableProfiler(
            profile_dataset=validator,
            excluded_expectations=None,
            ignored_columns=ignored_columns,
            not_null_only=False,
            primary_or_compound_key=None,
            semantic_types_dict=None,
            table_expectations_only=False,
            value_set_threshold="MANY",
        )
        suite = profiler.build_suite()
        validator.expectation_suite = suite
        # print('==========')
        # print(validator.get_expectation_suite(discard_failed_expectations=False))
        validator.save_expectation_suite(discard_failed_expectations=False)

    def check_2(self):
        # 加载内存方法2
        checkpoint_config = {
            "name": "my_missing_batch_request_checkpoint",
            "run_name_template": "template",
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "expectation_suite_name": self.expectation_suite_name,
        }
        self.context.add_checkpoint(**checkpoint_config)
        df = self.get_data_1()

        batch_request = RuntimeBatchRequest(
            datasource_name=self.datasource_name,
            data_connector_name=self.runtime_connector_name,
            data_asset_name=self.template_file,  # This can be anything that identifies this data_asset for you
            runtime_parameters={"batch_data": df},  # Pass your DataFrame here.
            batch_identifiers={"default_identifier_name": "batch_id"},
        )

        results = self.context.run_checkpoint(
            checkpoint_name="my_missing_batch_request_checkpoint",
            validations=[{"batch_request": batch_request},],
        )

    def run_2(self):
        self.data_source()
        self.suite()
        self.check_2()

```
### 示例-自定义发送datahub
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669106827819-ae4df3b2-6305-413f-b611-1e2e0c02667a.png#averageHue=%23fdfdfc&clientId=u815a5b7c-1b69-4&from=paste&height=317&id=u99c149fc&originHeight=741&originWidth=1187&originalType=binary&ratio=1&rotation=0&showTitle=false&size=72036&status=done&style=none&taskId=u4da482e9-05d2-4d63-8e22-a6c1edc82d3&title=&width=507)
```python
    def run(self):
        store_backend_defaults = InMemoryStoreBackendDefaults()
        data_context_config = DataContextConfig(
            store_backend_defaults=store_backend_defaults,
            checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
        )
        context = BaseDataContext(project_config=data_context_config)
        context._usage_statistics_handler = None

        datasource_config = {
            "name": "postgres",
            "class_name": "Datasource",
            "execution_engine": {"class_name": "PandasExecutionEngine"},
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"],
                }
            },
        }
        context.test_yaml_config(yaml.dump(datasource_config))
        context.add_datasource(**datasource_config)

        data = pd.DataFrame(
            {"account_id": ["a", "b"], "log_date": ["2022-01-01", "2022-01-02"]}
        )
        batch_request = RuntimeBatchRequest(
            datasource_name="postgres",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=f"public.result_facebook",
            batch_identifiers={"batch_id": "default_identifier"},
            runtime_parameters={"batch_data": data},
        )

        expectation_suite_name: str = f"test_topic"
        context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name, overwrite_existing=True
        )
        validator = context.get_validator(
            batch_request=batch_request, expectation_suite_name=expectation_suite_name,
        )
        result = getattr(validator, "expect_column_values_to_be_in_set")(
            **{"column": "account_id", "value_set": ["a"]}
        )
        expectation = validator.validate()
        # 由 DataHubValidationAction 重写
        DataHubValidation()._run(
            validation_result_suite=expectation,
            data_asset=validator,
            platform_name="dwd",
        )
```
## 文档

- [Integrating DataHub With Great Expectations](https://docs.greatexpectations.io/docs/integrations/integration_datahub)
- [Explore Expectations](https://greatexpectations.io/expectations/)
- [Expectation implementations by backend](https://docs.greatexpectations.io/docs/reference/expectations/implemented_expectations)

# 

