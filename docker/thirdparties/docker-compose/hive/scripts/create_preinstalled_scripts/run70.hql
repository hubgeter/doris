use `default`;


CREATE TABLE json_all_complex_types (
  `id` int,
  `boolean_col` boolean, 
  `tinyint_col` tinyint, 
  `smallint_col` smallint, 
  `int_col` int, 
  `bigint_col` bigint, 
  `float_col` float, 
  `double_col` double, 
  `decimal_col1` decimal(9,0), 
  `decimal_col2` decimal(8,4), 
  `decimal_col3` decimal(18,6), 
  `decimal_col4` decimal(38,12), 
  `string_col` string, 
  `binary_col` binary, 
  `date_col` date, 
  `timestamp_col1` timestamp, 
  `timestamp_col2` timestamp, 
  `timestamp_col3` timestamp, 
  `char_col1` char(50), 
  `char_col2` char(100), 
  `char_col3` char(255), 
  `varchar_col1` varchar(50), 
  `varchar_col2` varchar(100), 
  `varchar_col3` varchar(255), 
  `t_map_string` map<string,string>, 
  `t_map_varchar` map<varchar(65535),varchar(65535)>, 
  `t_map_char` map<char(10),char(10)>, 
  `t_map_int` map<int,int>, 
  `t_map_bigint` map<bigint,bigint>, 
  `t_map_float` map<float,float>, 
  `t_map_double` map<double,double>, 
  `t_map_boolean` map<boolean,boolean>, 
  `t_map_decimal_precision_2` map<decimal(2,1),decimal(2,1)>, 
  `t_map_decimal_precision_4` map<decimal(4,2),decimal(4,2)>, 
  `t_map_decimal_precision_8` map<decimal(8,4),decimal(8,4)>, 
  `t_map_decimal_precision_17` map<decimal(17,8),decimal(17,8)>, 
  `t_map_decimal_precision_18` map<decimal(18,8),decimal(18,8)>, 
  `t_map_decimal_precision_38` map<decimal(38,16),decimal(38,16)>, 
  `t_array_string` array<string>, 
  `t_array_int` array<int>, 
  `t_array_bigint` array<bigint>, 
  `t_array_float` array<float>, 
  `t_array_double` array<double>, 
  `t_array_boolean` array<boolean>, 
  `t_array_varchar` array<varchar(65535)>, 
  `t_array_char` array<char(10)>, 
  `t_array_decimal_precision_2` array<decimal(2,1)>, 
  `t_array_decimal_precision_4` array<decimal(4,2)>, 
  `t_array_decimal_precision_8` array<decimal(8,4)>, 
  `t_array_decimal_precision_17` array<decimal(17,8)>, 
  `t_array_decimal_precision_18` array<decimal(18,8)>, 
  `t_array_decimal_precision_38` array<decimal(38,16)>, 
  `t_struct_bigint` struct<s_bigint:bigint>, 
  `t_complex` map<string,array<struct<s_int:int>>>, 
  `t_struct_nested` struct<struct_field:array<string>>, 
  `t_struct_null` struct<struct_field_null:string,struct_field_null2:string>, 
  `t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>, 
  `t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>, 
  `t_map_null_value` map<string,string>, 
  `t_array_string_starting_with_nulls` array<string>, 
  `t_array_string_with_nulls_in_between` array<string>, 
  `t_array_string_ending_with_nulls` array<string>, 
  `t_array_string_all_nulls` array<string>
    ) PARTITIONED BY (`dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' 
LOCATION
  '/user/doris/preinstalled_data/json/json_all_complex_types';

msck repair table json_all_complex_types;
