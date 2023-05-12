## ETL on `OpenPowerlifting` Data


```python
from IPython.display import display, Markdown
import polars as pl 
from datetime import datetime as dt

# read configs 
import sys
from pathlib import Path
sys.path.append(str(Path().resolve().parent))
from steps import conf
```

### Loading Data


```python
s3_file_path = f"https://{conf.bucket_name}.s3.ap-southeast-2.amazonaws.com/{conf.parquet_file}"

df = pl.read_parquet(s3_file_path)
df.head(5)
```




<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
}
</style>
<small>shape: (5, 41)</small><table border="1" class="dataframe"><thead><tr><th>Name</th><th>Sex</th><th>Event</th><th>Equipment</th><th>Age</th><th>AgeClass</th><th>BirthYearClass</th><th>Division</th><th>BodyweightKg</th><th>WeightClassKg</th><th>Squat1Kg</th><th>Squat2Kg</th><th>Squat3Kg</th><th>Squat4Kg</th><th>Best3SquatKg</th><th>Bench1Kg</th><th>Bench2Kg</th><th>Bench3Kg</th><th>Bench4Kg</th><th>Best3BenchKg</th><th>Deadlift1Kg</th><th>Deadlift2Kg</th><th>Deadlift3Kg</th><th>Deadlift4Kg</th><th>Best3DeadliftKg</th><th>TotalKg</th><th>Place</th><th>Dots</th><th>Wilks</th><th>Glossbrenner</th><th>Goodlift</th><th>Tested</th><th>Country</th><th>State</th><th>Federation</th><th>ParentFederation</th><th>Date</th><th>MeetCountry</th><th>MeetState</th><th>MeetTown</th><th>MeetName</th></tr><tr><td>str</td><td>str</td><td>str</td><td>str</td><td>f64</td><td>str</td><td>str</td><td>str</td><td>f64</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td></tr></thead><tbody><tr><td>&quot;Alona Vladi&quot;</td><td>&quot;F&quot;</td><td>&quot;SBD&quot;</td><td>&quot;Raw&quot;</td><td>33.0</td><td>&quot;24-34&quot;</td><td>&quot;24-39&quot;</td><td>&quot;O&quot;</td><td>58.3</td><td>&quot;60&quot;</td><td>75.0</td><td>80.0</td><td>-90.0</td><td>null</td><td>80.0</td><td>50.0</td><td>55.0</td><td>60.0</td><td>null</td><td>60.0</td><td>95.0</td><td>105.0</td><td>107.5</td><td>null</td><td>107.5</td><td>247.5</td><td>&quot;1&quot;</td><td>279.44</td><td>282.18</td><td>249.42</td><td>57.1</td><td>&quot;Yes&quot;</td><td>&quot;Russia&quot;</td><td>null</td><td>&quot;GFP&quot;</td><td>null</td><td>&quot;2019-05-11&quot;</td><td>&quot;Russia&quot;</td><td>null</td><td>&quot;Bryansk&quot;</td><td>&quot;Open Tournamen…</td></tr><tr><td>&quot;Galina Solovya…</td><td>&quot;F&quot;</td><td>&quot;SBD&quot;</td><td>&quot;Raw&quot;</td><td>43.0</td><td>&quot;40-44&quot;</td><td>&quot;40-49&quot;</td><td>&quot;M1&quot;</td><td>73.1</td><td>&quot;75&quot;</td><td>95.0</td><td>100.0</td><td>105.0</td><td>null</td><td>105.0</td><td>62.5</td><td>67.5</td><td>-72.5</td><td>null</td><td>67.5</td><td>100.0</td><td>110.0</td><td>-120.0</td><td>null</td><td>110.0</td><td>282.5</td><td>&quot;1&quot;</td><td>278.95</td><td>272.99</td><td>240.35</td><td>56.76</td><td>&quot;Yes&quot;</td><td>&quot;Russia&quot;</td><td>null</td><td>&quot;GFP&quot;</td><td>null</td><td>&quot;2019-05-11&quot;</td><td>&quot;Russia&quot;</td><td>null</td><td>&quot;Bryansk&quot;</td><td>&quot;Open Tournamen…</td></tr><tr><td>&quot;Daniil Voronin…</td><td>&quot;M&quot;</td><td>&quot;SBD&quot;</td><td>&quot;Raw&quot;</td><td>15.5</td><td>&quot;16-17&quot;</td><td>&quot;14-18&quot;</td><td>&quot;T&quot;</td><td>67.4</td><td>&quot;75&quot;</td><td>85.0</td><td>90.0</td><td>100.0</td><td>null</td><td>100.0</td><td>55.0</td><td>62.5</td><td>-65.0</td><td>null</td><td>62.5</td><td>90.0</td><td>100.0</td><td>105.0</td><td>null</td><td>105.0</td><td>267.5</td><td>&quot;1&quot;</td><td>206.4</td><td>206.49</td><td>200.45</td><td>41.24</td><td>&quot;Yes&quot;</td><td>&quot;Russia&quot;</td><td>null</td><td>&quot;GFP&quot;</td><td>null</td><td>&quot;2019-05-11&quot;</td><td>&quot;Russia&quot;</td><td>null</td><td>&quot;Bryansk&quot;</td><td>&quot;Open Tournamen…</td></tr><tr><td>&quot;Aleksey Krasov…</td><td>&quot;M&quot;</td><td>&quot;SBD&quot;</td><td>&quot;Raw&quot;</td><td>35.0</td><td>&quot;35-39&quot;</td><td>&quot;24-39&quot;</td><td>&quot;O&quot;</td><td>66.65</td><td>&quot;75&quot;</td><td>125.0</td><td>132.0</td><td>137.5</td><td>null</td><td>137.5</td><td>115.0</td><td>122.5</td><td>-127.5</td><td>null</td><td>122.5</td><td>150.0</td><td>165.0</td><td>170.0</td><td>null</td><td>170.0</td><td>430.0</td><td>&quot;1&quot;</td><td>334.49</td><td>334.94</td><td>325.32</td><td>66.68</td><td>&quot;Yes&quot;</td><td>&quot;Russia&quot;</td><td>null</td><td>&quot;GFP&quot;</td><td>null</td><td>&quot;2019-05-11&quot;</td><td>&quot;Russia&quot;</td><td>null</td><td>&quot;Bryansk&quot;</td><td>&quot;Open Tournamen…</td></tr><tr><td>&quot;Margarita Ples…</td><td>&quot;M&quot;</td><td>&quot;SBD&quot;</td><td>&quot;Raw&quot;</td><td>26.5</td><td>&quot;24-34&quot;</td><td>&quot;24-39&quot;</td><td>&quot;O&quot;</td><td>72.45</td><td>&quot;75&quot;</td><td>80.0</td><td>85.0</td><td>90.0</td><td>null</td><td>90.0</td><td>40.0</td><td>50.0</td><td>-60.0</td><td>null</td><td>50.0</td><td>112.5</td><td>120.0</td><td>125.0</td><td>null</td><td>125.0</td><td>265.0</td><td>&quot;1&quot;</td><td>194.46</td><td>193.55</td><td>187.29</td><td>39.34</td><td>&quot;Yes&quot;</td><td>&quot;Russia&quot;</td><td>null</td><td>&quot;GFP&quot;</td><td>null</td><td>&quot;2019-05-11&quot;</td><td>&quot;Russia&quot;</td><td>null</td><td>&quot;Bryansk&quot;</td><td>&quot;Open Tournamen…</td></tr></tbody></table></div>




```python
cleansing_data_md = f"""
## Cleansing data
- Filter for only particular columns from {conf.op_cols}
    - Select events that are IPF / Tested federations only (i.e. `Tested` = "Yes")
    - Remove anyone who has been disqualified (i.e. `Place` = "DQ")
    - Only `Raw` equipment events
- Drop any rows that have missing values in the columns above.
- Drop any duplicates based on all columns.
"""

display(Markdown(cleansing_data_md))
```



## Cleansing data
- Filter for only particular columns from ['Date', 'Name', 'Sex', 'Place', 'Age', 'AgeClass', 'BodyweightKg', 'Event', 'MeetCountry', 'Equipment', 'Best3SquatKg', 'Best3BenchKg', 'Best3DeadliftKg', 'TotalKg', 'Wilks', 'Tested', 'Federation', 'MeetName']
    - Select events that are IPF / Tested federations only (i.e. `Tested` = "Yes")
    - Remove anyone who has been disqualified (i.e. `Place` = "DQ")
    - Only `Raw` equipment events
- Drop any rows that have missing values in the columns above.
- Drop any duplicates based on all columns.




```python
base_df = df.select(conf.op_cols)
print(base_df.shape)
base_df.head(5)
```

    (2903147, 18)





<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
}
</style>
<small>shape: (5, 18)</small><table border="1" class="dataframe"><thead><tr><th>Date</th><th>Name</th><th>Sex</th><th>Place</th><th>Age</th><th>AgeClass</th><th>BodyweightKg</th><th>Event</th><th>MeetCountry</th><th>Equipment</th><th>Best3SquatKg</th><th>Best3BenchKg</th><th>Best3DeadliftKg</th><th>TotalKg</th><th>Wilks</th><th>Tested</th><th>Federation</th><th>MeetName</th></tr><tr><td>str</td><td>str</td><td>str</td><td>str</td><td>f64</td><td>str</td><td>f64</td><td>str</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>str</td><td>str</td><td>str</td></tr></thead><tbody><tr><td>&quot;2019-05-11&quot;</td><td>&quot;Alona Vladi&quot;</td><td>&quot;F&quot;</td><td>&quot;1&quot;</td><td>33.0</td><td>&quot;24-34&quot;</td><td>58.3</td><td>&quot;SBD&quot;</td><td>&quot;Russia&quot;</td><td>&quot;Raw&quot;</td><td>80.0</td><td>60.0</td><td>107.5</td><td>247.5</td><td>282.18</td><td>&quot;Yes&quot;</td><td>&quot;GFP&quot;</td><td>&quot;Open Tournamen…</td></tr><tr><td>&quot;2019-05-11&quot;</td><td>&quot;Galina Solovya…</td><td>&quot;F&quot;</td><td>&quot;1&quot;</td><td>43.0</td><td>&quot;40-44&quot;</td><td>73.1</td><td>&quot;SBD&quot;</td><td>&quot;Russia&quot;</td><td>&quot;Raw&quot;</td><td>105.0</td><td>67.5</td><td>110.0</td><td>282.5</td><td>272.99</td><td>&quot;Yes&quot;</td><td>&quot;GFP&quot;</td><td>&quot;Open Tournamen…</td></tr><tr><td>&quot;2019-05-11&quot;</td><td>&quot;Daniil Voronin…</td><td>&quot;M&quot;</td><td>&quot;1&quot;</td><td>15.5</td><td>&quot;16-17&quot;</td><td>67.4</td><td>&quot;SBD&quot;</td><td>&quot;Russia&quot;</td><td>&quot;Raw&quot;</td><td>100.0</td><td>62.5</td><td>105.0</td><td>267.5</td><td>206.49</td><td>&quot;Yes&quot;</td><td>&quot;GFP&quot;</td><td>&quot;Open Tournamen…</td></tr><tr><td>&quot;2019-05-11&quot;</td><td>&quot;Aleksey Krasov…</td><td>&quot;M&quot;</td><td>&quot;1&quot;</td><td>35.0</td><td>&quot;35-39&quot;</td><td>66.65</td><td>&quot;SBD&quot;</td><td>&quot;Russia&quot;</td><td>&quot;Raw&quot;</td><td>137.5</td><td>122.5</td><td>170.0</td><td>430.0</td><td>334.94</td><td>&quot;Yes&quot;</td><td>&quot;GFP&quot;</td><td>&quot;Open Tournamen…</td></tr><tr><td>&quot;2019-05-11&quot;</td><td>&quot;Margarita Ples…</td><td>&quot;M&quot;</td><td>&quot;1&quot;</td><td>26.5</td><td>&quot;24-34&quot;</td><td>72.45</td><td>&quot;SBD&quot;</td><td>&quot;Russia&quot;</td><td>&quot;Raw&quot;</td><td>90.0</td><td>50.0</td><td>125.0</td><td>265.0</td><td>193.55</td><td>&quot;Yes&quot;</td><td>&quot;GFP&quot;</td><td>&quot;Open Tournamen…</td></tr></tbody></table></div>




```python
cleansed_df = base_df.filter(
    (pl.col("Event") == "SBD") & 
    (pl.col("Tested") == "Yes") & 
    (pl.col('Place').apply(lambda x: x.isnumeric(), return_dtype = pl.Boolean)) & 
    (pl.col("Equipment") == "Raw")
).drop_nulls().unique().sort("Date", descending=True).drop(["Tested", "Federation", "Event"])
print(cleansed_df.shape)
cleansed_df.head(5)
```

    (427224, 15)





<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
}
</style>
<small>shape: (5, 15)</small><table border="1" class="dataframe"><thead><tr><th>Date</th><th>Name</th><th>Sex</th><th>Place</th><th>Age</th><th>AgeClass</th><th>BodyweightKg</th><th>MeetCountry</th><th>Equipment</th><th>Best3SquatKg</th><th>Best3BenchKg</th><th>Best3DeadliftKg</th><th>TotalKg</th><th>Wilks</th><th>MeetName</th></tr><tr><td>str</td><td>str</td><td>str</td><td>str</td><td>f64</td><td>str</td><td>f64</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>str</td></tr></thead><tbody><tr><td>&quot;2023-04-30&quot;</td><td>&quot;Matyas Kovach&quot;</td><td>&quot;M&quot;</td><td>&quot;1&quot;</td><td>41.0</td><td>&quot;40-44&quot;</td><td>73.85</td><td>&quot;USA&quot;</td><td>&quot;Raw&quot;</td><td>180.0</td><td>132.5</td><td>185.0</td><td>497.5</td><td>358.37</td><td>&quot;Tri-State Cham…</td></tr><tr><td>&quot;2023-04-30&quot;</td><td>&quot;Amanda Lairmor…</td><td>&quot;F&quot;</td><td>&quot;1&quot;</td><td>23.5</td><td>&quot;24-34&quot;</td><td>50.4</td><td>&quot;USA&quot;</td><td>&quot;Raw&quot;</td><td>160.0</td><td>77.5</td><td>120.0</td><td>357.5</td><td>456.49</td><td>&quot;MetroEast Rook…</td></tr><tr><td>&quot;2023-04-30&quot;</td><td>&quot;Wendolyn Reyes…</td><td>&quot;F&quot;</td><td>&quot;2&quot;</td><td>21.5</td><td>&quot;20-23&quot;</td><td>103.1</td><td>&quot;USA&quot;</td><td>&quot;Raw&quot;</td><td>150.0</td><td>75.0</td><td>175.0</td><td>400.0</td><td>330.22</td><td>&quot;MetroEast Rook…</td></tr><tr><td>&quot;2023-04-30&quot;</td><td>&quot;Mikaela Inskee…</td><td>&quot;F&quot;</td><td>&quot;3&quot;</td><td>19.5</td><td>&quot;20-23&quot;</td><td>59.2</td><td>&quot;USA&quot;</td><td>&quot;Raw&quot;</td><td>92.5</td><td>65.0</td><td>125.0</td><td>282.5</td><td>318.26</td><td>&quot;MetroEast Rook…</td></tr><tr><td>&quot;2023-04-30&quot;</td><td>&quot;Alexis Novak&quot;</td><td>&quot;F&quot;</td><td>&quot;4&quot;</td><td>21.5</td><td>&quot;20-23&quot;</td><td>62.8</td><td>&quot;USA&quot;</td><td>&quot;Raw&quot;</td><td>90.0</td><td>55.0</td><td>102.5</td><td>247.5</td><td>266.45</td><td>&quot;MetroEast Rook…</td></tr></tbody></table></div>




```python
cleansed_df.filter(pl.col("Name") == "John Paul Cauchi").sort("Date", descending=True).head(5)
```




<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
}
</style>
<small>shape: (5, 15)</small><table border="1" class="dataframe"><thead><tr><th>Date</th><th>Name</th><th>Sex</th><th>Place</th><th>Age</th><th>AgeClass</th><th>BodyweightKg</th><th>MeetCountry</th><th>Equipment</th><th>Best3SquatKg</th><th>Best3BenchKg</th><th>Best3DeadliftKg</th><th>TotalKg</th><th>Wilks</th><th>MeetName</th></tr><tr><td>str</td><td>str</td><td>str</td><td>str</td><td>f64</td><td>str</td><td>f64</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>str</td></tr></thead><tbody><tr><td>&quot;2022-11-05&quot;</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>&quot;1&quot;</td><td>29.5</td><td>&quot;24-34&quot;</td><td>82.35</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>265.0</td><td>137.5</td><td>290.0</td><td>692.5</td><td>464.42</td><td>&quot;Ballarat Open&quot;</td></tr><tr><td>&quot;2021-04-11&quot;</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>&quot;1&quot;</td><td>28.0</td><td>&quot;24-34&quot;</td><td>76.75</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>260.0</td><td>138.0</td><td>290.0</td><td>688.0</td><td>482.56</td><td>&quot;JPS Open VII&quot;</td></tr><tr><td>&quot;2020-09-26&quot;</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>&quot;2&quot;</td><td>27.5</td><td>&quot;24-34&quot;</td><td>76.95</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>256.0</td><td>133.0</td><td>306.0</td><td>695.0</td><td>486.62</td><td>&quot;Australian Pow…</td></tr><tr><td>&quot;2019-08-22&quot;</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>&quot;2&quot;</td><td>26.5</td><td>&quot;24-34&quot;</td><td>76.9</td><td>&quot;China&quot;</td><td>&quot;Raw&quot;</td><td>248.0</td><td>135.0</td><td>273.0</td><td>656.0</td><td>459.52</td><td>&quot;Asia Pacific O…</td></tr><tr><td>&quot;2019-06-28&quot;</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>&quot;1&quot;</td><td>26.5</td><td>&quot;24-34&quot;</td><td>76.85</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>253.0</td><td>134.0</td><td>286.0</td><td>673.0</td><td>471.63</td><td>&quot;Australian Pow…</td></tr></tbody></table></div>



## Data Preparation
- Drop the `Tested`, `Federation` and `Event` columns as they are no longer needed.
- Update column types: 
    - `Date` to `Date`
    - `Place` to `Int64`
- Collect data from 2000-01-01 onwards. 
- Rename columns from camel to snake case 
- Assume that a powerlifter's country is from the first country that compete in. 


```python
# find the first country that the powerlifter competed in and assume that is their country of origin
lifter_country_df = cleansed_df.groupby(["Name", "Sex"]).agg(pl.first("MeetCountry").alias("OriginCountry"))
```


```python
data_prep_df = cleansed_df.sort(["Name", "Date"], descending=[False, True]).join(lifter_country_df, on=["Name", "Sex"]).filter(pl.col("Date").gt("2000-01-01")).with_columns(
    pl.col("Date").str.strptime(pl.Date, fmt="%Y-%m-%d").alias("Date"),
    pl.col("Place").cast(pl.Int32).alias("Place"),
).rename(
    mapping=conf.op_cols_rename
).select(
    pl.all().map_alias(lambda col_name: conf.camel_to_snake(col_name))
)


print(data_prep_df.shape)
data_prep_df.head(5)
```

    (426396, 16)





<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
}
</style>
<small>shape: (5, 16)</small><table border="1" class="dataframe"><thead><tr><th>date</th><th>name</th><th>sex</th><th>place</th><th>age</th><th>age_class</th><th>bodyweight</th><th>meet_country</th><th>equipment</th><th>squat</th><th>bench</th><th>deadlift</th><th>total</th><th>wilks</th><th>meet_name</th><th>origin_country</th></tr><tr><td>date</td><td>str</td><td>str</td><td>i32</td><td>f64</td><td>str</td><td>f64</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>str</td><td>str</td></tr></thead><tbody><tr><td>2017-12-04</td><td>&quot;A Ajeesha&quot;</td><td>&quot;F&quot;</td><td>1</td><td>16.5</td><td>&quot;16-17&quot;</td><td>71.1</td><td>&quot;India&quot;</td><td>&quot;Raw&quot;</td><td>112.5</td><td>55.0</td><td>132.5</td><td>300.0</td><td>295.29</td><td>&quot;Asian Classic …</td><td>&quot;India&quot;</td></tr><tr><td>2012-12-10</td><td>&quot;A Ashwin&quot;</td><td>&quot;M&quot;</td><td>1</td><td>16.5</td><td>&quot;16-17&quot;</td><td>82.55</td><td>&quot;India&quot;</td><td>&quot;Raw&quot;</td><td>170.0</td><td>95.0</td><td>220.0</td><td>485.0</td><td>324.79</td><td>&quot;Asian Classic …</td><td>&quot;India&quot;</td></tr><tr><td>2019-10-01</td><td>&quot;A Belousov&quot;</td><td>&quot;M&quot;</td><td>8</td><td>17.5</td><td>&quot;18-19&quot;</td><td>73.6</td><td>&quot;Kazakhstan&quot;</td><td>&quot;Raw&quot;</td><td>75.0</td><td>75.0</td><td>100.0</td><td>250.0</td><td>180.52</td><td>&quot;Kazakhstan Cla…</td><td>&quot;Kazakhstan&quot;</td></tr><tr><td>2019-09-26</td><td>&quot;A K S Shri Ram…</td><td>&quot;M&quot;</td><td>13</td><td>16.0</td><td>&quot;16-17&quot;</td><td>78.2</td><td>&quot;India&quot;</td><td>&quot;Raw&quot;</td><td>117.5</td><td>50.0</td><td>150.0</td><td>317.5</td><td>219.95</td><td>&quot;Indian Classic…</td><td>&quot;India&quot;</td></tr><tr><td>2019-09-26</td><td>&quot;A Pradeep&quot;</td><td>&quot;M&quot;</td><td>6</td><td>17.0</td><td>&quot;16-17&quot;</td><td>80.7</td><td>&quot;India&quot;</td><td>&quot;Raw&quot;</td><td>150.0</td><td>97.5</td><td>170.0</td><td>417.5</td><td>283.48</td><td>&quot;Indian Classic…</td><td>&quot;India&quot;</td></tr></tbody></table></div>



### Feature Engineering
- Create a `pot_*` (progress over time) columns for `wilks` and `total`
- Adds columns:
  - `time_since_last_comp`: identify how long it has been since their last competition (in days)
  - `home_country`: 1 if `meet_country` == `origin_country` else 0 
- Switches `Date` 


```python
fe_df = data_prep_df.with_columns(
     (pl.col('date') - pl.col('date').shift(-1)).over('name').alias('time_since_last_comp').apply(lambda x: x.days).cast(pl.Int32)
).sort(
    ["name", "date"], descending=[False, False]
).with_columns(
    (pl.col('time_since_last_comp') / 365.25).alias('years_since_last_comp')
)

def progress_per_year(df: pl.DataFrame) -> pl.DataFrame:
    df = df.sort('date')
    for lift in ['squat', 'bench', 'deadlift', 'total', 'wilks']:
        df = df.with_columns(
            ((pl.col(lift) - pl.col(lift).shift(1)) / pl.col('years_since_last_comp')).alias(f'{lift}_progress')
        )
    return df


# Groupby each lifter's lifts 
lifters_df = fe_df.select(
    pl.col(['date', 'name', 'squat', 'bench', 'deadlift', 'total', 'wilks', 'time_since_last_comp'])
)

# Calculate progress per day for each column
# fe_df = fe_df.with_columns([
#     progress_per_year(pl.col('squat'), pl.col('time_since_last_comp')).alias('squat_progress'),
#     progress_per_year(pl.col('bench'), pl.col('time_since_last_comp')).alias('bench_progress'),
#     progress_per_year(pl.col('deadlift'), pl.col('time_since_last_comp')).alias('deadlift_progress'),
#     progress_per_year(pl.col('total'), pl.col('time_since_last_comp')).alias('total_progress'),
#     progress_per_year(pl.col('wilks'), pl.col('time_since_last_comp')).alias('wilks_progress'),
#     (pl.col("meet_country") == pl.col("origin_country")).alias("is_origin_country"),
#     pl.col('date').apply(lambda x: x.toordinal()).alias('date_as_ordinal')
# ]).drop_nulls()

```


```python
jp_df = fe_df.filter(pl.col('name') == 'John Paul Cauchi')
jp_df.head(2)
```




<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
}
</style>
<small>shape: (2, 18)</small><table border="1" class="dataframe"><thead><tr><th>date</th><th>name</th><th>sex</th><th>place</th><th>age</th><th>age_class</th><th>bodyweight</th><th>meet_country</th><th>equipment</th><th>squat</th><th>bench</th><th>deadlift</th><th>total</th><th>wilks</th><th>meet_name</th><th>origin_country</th><th>time_since_last_comp</th><th>years_since_last_comp</th></tr><tr><td>date</td><td>str</td><td>str</td><td>i32</td><td>f64</td><td>str</td><td>f64</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>str</td><td>str</td><td>i32</td><td>f64</td></tr></thead><tbody><tr><td>2013-02-17</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>20.0</td><td>&quot;20-23&quot;</td><td>64.4</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>165.0</td><td>75.0</td><td>190.0</td><td>430.0</td><td>344.61</td><td>&quot;Melbourne Summ…</td><td>&quot;Australia&quot;</td><td>null</td><td>null</td></tr><tr><td>2013-06-11</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>2</td><td>20.5</td><td>&quot;20-23&quot;</td><td>65.6</td><td>&quot;Russia&quot;</td><td>&quot;Raw&quot;</td><td>185.0</td><td>100.0</td><td>220.0</td><td>505.0</td><td>398.52</td><td>&quot;World Classic …</td><td>&quot;Australia&quot;</td><td>114</td><td>0.312115</td></tr></tbody></table></div>




```python
jp_df.groupby('name').apply(progress_per_year)
```




<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
}
</style>
<small>shape: (25, 23)</small><table border="1" class="dataframe"><thead><tr><th>date</th><th>name</th><th>sex</th><th>place</th><th>age</th><th>age_class</th><th>bodyweight</th><th>meet_country</th><th>equipment</th><th>squat</th><th>bench</th><th>deadlift</th><th>total</th><th>wilks</th><th>meet_name</th><th>origin_country</th><th>time_since_last_comp</th><th>years_since_last_comp</th><th>squat_progress</th><th>bench_progress</th><th>deadlift_progress</th><th>total_progress</th><th>wilks_progress</th></tr><tr><td>date</td><td>str</td><td>str</td><td>i32</td><td>f64</td><td>str</td><td>f64</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>str</td><td>str</td><td>i32</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>2013-02-17</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>20.0</td><td>&quot;20-23&quot;</td><td>64.4</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>165.0</td><td>75.0</td><td>190.0</td><td>430.0</td><td>344.61</td><td>&quot;Melbourne Summ…</td><td>&quot;Australia&quot;</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr><tr><td>2013-06-11</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>2</td><td>20.5</td><td>&quot;20-23&quot;</td><td>65.6</td><td>&quot;Russia&quot;</td><td>&quot;Raw&quot;</td><td>185.0</td><td>100.0</td><td>220.0</td><td>505.0</td><td>398.52</td><td>&quot;World Classic …</td><td>&quot;Australia&quot;</td><td>114</td><td>0.312115</td><td>64.078947</td><td>80.098684</td><td>96.118421</td><td>240.296053</td><td>172.724803</td></tr><tr><td>2013-08-18</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>20.5</td><td>&quot;20-23&quot;</td><td>65.7</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>195.0</td><td>100.0</td><td>217.5</td><td>512.5</td><td>403.93</td><td>&quot;Battler&#x27;s Novi…</td><td>&quot;Australia&quot;</td><td>68</td><td>0.186174</td><td>53.713235</td><td>0.0</td><td>-13.428309</td><td>40.284926</td><td>29.05886</td></tr><tr><td>2013-12-05</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>2</td><td>20.5</td><td>&quot;20-23&quot;</td><td>65.5</td><td>&quot;New Zealand&quot;</td><td>&quot;Raw&quot;</td><td>200.0</td><td>105.0</td><td>230.0</td><td>535.0</td><td>422.73</td><td>&quot;Commonwealth &amp;…</td><td>&quot;Australia&quot;</td><td>109</td><td>0.298426</td><td>16.754587</td><td>16.754587</td><td>41.886468</td><td>75.395642</td><td>62.997248</td></tr><tr><td>2014-04-04</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>2</td><td>21.0</td><td>&quot;20-23&quot;</td><td>65.5</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>205.5</td><td>107.5</td><td>250.0</td><td>563.0</td><td>444.85</td><td>&quot;Australian Raw…</td><td>&quot;Australia&quot;</td><td>120</td><td>0.328542</td><td>16.740625</td><td>7.609375</td><td>60.875</td><td>85.225</td><td>67.32775</td></tr><tr><td>2014-06-01</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>21.5</td><td>&quot;20-23&quot;</td><td>65.75</td><td>&quot;South Africa&quot;</td><td>&quot;Raw&quot;</td><td>210.0</td><td>110.0</td><td>273.0</td><td>593.0</td><td>467.08</td><td>&quot;World Classic …</td><td>&quot;Australia&quot;</td><td>58</td><td>0.158795</td><td>28.338362</td><td>15.743534</td><td>144.840517</td><td>188.922414</td><td>139.991509</td></tr><tr><td>2014-08-22</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>21.5</td><td>&quot;20-23&quot;</td><td>73.4</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>228.0</td><td>117.5</td><td>255.0</td><td>600.5</td><td>434.46</td><td>&quot;Australian Raw…</td><td>&quot;Australia&quot;</td><td>82</td><td>0.224504</td><td>80.176829</td><td>33.407012</td><td>-80.176829</td><td>33.407012</td><td>-145.298232</td></tr><tr><td>2014-12-08</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>21.5</td><td>&quot;20-23&quot;</td><td>73.55</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>230.0</td><td>120.0</td><td>277.5</td><td>627.5</td><td>453.34</td><td>&quot;Asia &amp; Oceania…</td><td>&quot;Australia&quot;</td><td>108</td><td>0.295688</td><td>6.763889</td><td>8.454861</td><td>76.09375</td><td>91.3125</td><td>63.851111</td></tr><tr><td>2015-04-10</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>22.0</td><td>&quot;20-23&quot;</td><td>66.0</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>215.0</td><td>112.5</td><td>280.0</td><td>607.5</td><td>477.01</td><td>&quot;Australian Raw…</td><td>&quot;Australia&quot;</td><td>123</td><td>0.336756</td><td>-44.542683</td><td>-22.271341</td><td>7.42378</td><td>-59.390244</td><td>70.288354</td></tr><tr><td>2015-06-05</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>22.5</td><td>&quot;20-23&quot;</td><td>65.64</td><td>&quot;Finland&quot;</td><td>&quot;Raw&quot;</td><td>225.5</td><td>115.0</td><td>275.0</td><td>615.5</td><td>485.47</td><td>&quot;World Classic …</td><td>&quot;Australia&quot;</td><td>56</td><td>0.15332</td><td>68.484375</td><td>16.305804</td><td>-32.611607</td><td>52.178571</td><td>55.178839</td></tr><tr><td>2015-08-14</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>22.5</td><td>&quot;20-23&quot;</td><td>66.0</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>210.0</td><td>110.0</td><td>275.0</td><td>595.0</td><td>467.19</td><td>&quot;Australian Jun…</td><td>&quot;Australia&quot;</td><td>70</td><td>0.19165</td><td>-80.876786</td><td>-26.089286</td><td>0.0</td><td>-106.966071</td><td>-95.382429</td></tr><tr><td>2015-12-13</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>22.5</td><td>&quot;20-23&quot;</td><td>65.95</td><td>&quot;Uzbekistan&quot;</td><td>&quot;Raw&quot;</td><td>217.5</td><td>120.0</td><td>277.5</td><td>615.0</td><td>483.2</td><td>&quot;Asia-Oceania R…</td><td>&quot;Australia&quot;</td><td>121</td><td>0.33128</td><td>22.639463</td><td>30.18595</td><td>7.546488</td><td>60.371901</td><td>48.327707</td></tr><tr><td>2016-06-19</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>6</td><td>23.5</td><td>&quot;24-34&quot;</td><td>65.84</td><td>&quot;USA&quot;</td><td>&quot;Raw&quot;</td><td>222.5</td><td>122.5</td><td>282.5</td><td>627.5</td><td>493.7</td><td>&quot;World Classic …</td><td>&quot;Australia&quot;</td><td>189</td><td>0.517454</td><td>9.662698</td><td>4.831349</td><td>9.662698</td><td>24.156746</td><td>20.291667</td></tr><tr><td>2016-10-21</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>7</td><td>23.5</td><td>&quot;24-34&quot;</td><td>73.95</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>232.5</td><td>117.5</td><td>257.5</td><td>607.5</td><td>437.19</td><td>&quot;Australian Ope…</td><td>&quot;Australia&quot;</td><td>124</td><td>0.339493</td><td>29.455645</td><td>-14.727823</td><td>-73.639113</td><td>-58.91129</td><td>-166.453851</td></tr><tr><td>2017-04-29</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>24.5</td><td>&quot;24-34&quot;</td><td>73.9</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>225.0</td><td>122.5</td><td>280.0</td><td>627.5</td><td>451.8</td><td>&quot;Pacific Invita…</td><td>&quot;Australia&quot;</td><td>190</td><td>0.520192</td><td>-14.417763</td><td>9.611842</td><td>43.253289</td><td>38.447368</td><td>28.085803</td></tr><tr><td>2017-10-13</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>3</td><td>24.5</td><td>&quot;24-34&quot;</td><td>74.0</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>227.5</td><td>125.0</td><td>280.5</td><td>633.0</td><td>455.33</td><td>&quot;Australia Nati…</td><td>&quot;Australia&quot;</td><td>167</td><td>0.457221</td><td>5.467814</td><td>5.467814</td><td>1.093563</td><td>12.029192</td><td>7.720554</td></tr><tr><td>2018-02-25</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>25.0</td><td>&quot;24-34&quot;</td><td>73.5</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>245.0</td><td>130.0</td><td>267.5</td><td>642.5</td><td>464.4</td><td>&quot;City Strength …</td><td>&quot;Australia&quot;</td><td>135</td><td>0.36961</td><td>47.347222</td><td>13.527778</td><td>-35.172222</td><td>25.702778</td><td>24.539389</td></tr><tr><td>2018-05-05</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>25.5</td><td>&quot;24-34&quot;</td><td>73.7</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>235.0</td><td>132.5</td><td>286.0</td><td>653.5</td><td>471.43</td><td>&quot;North Queensla…</td><td>&quot;Australia&quot;</td><td>69</td><td>0.188912</td><td>-52.934783</td><td>13.233696</td><td>97.929348</td><td>58.228261</td><td>37.213152</td></tr><tr><td>2018-10-13</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>5</td><td>25.5</td><td>&quot;24-34&quot;</td><td>76.65</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>247.5</td><td>132.5</td><td>288.0</td><td>667.5</td><td>468.6</td><td>&quot;Australian Nat…</td><td>&quot;Australia&quot;</td><td>161</td><td>0.440794</td><td>28.357919</td><td>0.0</td><td>4.537267</td><td>31.76087</td><td>-6.420233</td></tr><tr><td>2018-12-08</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>25.5</td><td>&quot;24-34&quot;</td><td>76.7</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>250.0</td><td>130.0</td><td>280.0</td><td>660.0</td><td>463.13</td><td>&quot;THJE Strength …</td><td>&quot;Australia&quot;</td><td>56</td><td>0.15332</td><td>16.305804</td><td>-16.305804</td><td>-52.178571</td><td>-48.917411</td><td>-35.677098</td></tr><tr><td>2019-06-28</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>26.5</td><td>&quot;24-34&quot;</td><td>76.85</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>253.0</td><td>134.0</td><td>286.0</td><td>673.0</td><td>471.63</td><td>&quot;Australian Pow…</td><td>&quot;Australia&quot;</td><td>202</td><td>0.553046</td><td>5.424505</td><td>7.232673</td><td>10.84901</td><td>23.506188</td><td>15.369431</td></tr><tr><td>2019-08-22</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>2</td><td>26.5</td><td>&quot;24-34&quot;</td><td>76.9</td><td>&quot;China&quot;</td><td>&quot;Raw&quot;</td><td>248.0</td><td>135.0</td><td>273.0</td><td>656.0</td><td>459.52</td><td>&quot;Asia Pacific O…</td><td>&quot;Australia&quot;</td><td>55</td><td>0.150582</td><td>-33.204545</td><td>6.640909</td><td>-86.331818</td><td>-112.895455</td><td>-80.421409</td></tr><tr><td>2020-09-26</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>2</td><td>27.5</td><td>&quot;24-34&quot;</td><td>76.95</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>256.0</td><td>133.0</td><td>306.0</td><td>695.0</td><td>486.62</td><td>&quot;Australian Pow…</td><td>&quot;Australia&quot;</td><td>401</td><td>1.097878</td><td>7.286783</td><td>-1.821696</td><td>30.05798</td><td>35.523067</td><td>24.683978</td></tr><tr><td>2021-04-11</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>28.0</td><td>&quot;24-34&quot;</td><td>76.75</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>260.0</td><td>138.0</td><td>290.0</td><td>688.0</td><td>482.56</td><td>&quot;JPS Open VII&quot;</td><td>&quot;Australia&quot;</td><td>197</td><td>0.539357</td><td>7.416244</td><td>9.270305</td><td>-29.664975</td><td>-12.978426</td><td>-7.527487</td></tr><tr><td>2022-11-05</td><td>&quot;John Paul Cauc…</td><td>&quot;M&quot;</td><td>1</td><td>29.5</td><td>&quot;24-34&quot;</td><td>82.35</td><td>&quot;Australia&quot;</td><td>&quot;Raw&quot;</td><td>265.0</td><td>137.5</td><td>290.0</td><td>692.5</td><td>464.42</td><td>&quot;Ballarat Open&quot;</td><td>&quot;Australia&quot;</td><td>573</td><td>1.568789</td><td>3.187173</td><td>-0.318717</td><td>0.0</td><td>2.868455</td><td>-11.563063</td></tr></tbody></table></div>




```python
lifters_df.with_columns(
    (pl.col('squat').shift(1) - pl.col('squat')).alias('squat_progress')
)
```




<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
}
</style>
<small>shape: (716340, 9)</small><table border="1" class="dataframe"><thead><tr><th>date</th><th>name</th><th>squat</th><th>bench</th><th>deadlift</th><th>total</th><th>wilks</th><th>time_since_last_comp</th><th>squat_progress</th></tr><tr><td>date</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>i32</td><td>f64</td></tr></thead><tbody><tr><td>2012-03-26</td><td>&quot;A Abduzhabarov…</td><td>155.0</td><td>110.0</td><td>170.0</td><td>435.0</td><td>312.9</td><td>null</td><td>null</td></tr><tr><td>2002-04-06</td><td>&quot;A Abrutis&quot;</td><td>310.0</td><td>210.0</td><td>322.5</td><td>842.5</td><td>481.21</td><td>null</td><td>-155.0</td></tr><tr><td>2003-04-05</td><td>&quot;A Abrutis&quot;</td><td>370.0</td><td>242.5</td><td>350.0</td><td>962.5</td><td>550.52</td><td>364</td><td>-60.0</td></tr><tr><td>2017-12-04</td><td>&quot;A Ajeesha&quot;</td><td>112.5</td><td>55.0</td><td>132.5</td><td>300.0</td><td>295.29</td><td>null</td><td>257.5</td></tr><tr><td>2005-04-09</td><td>&quot;A Arlavicius&quot;</td><td>235.0</td><td>172.5</td><td>230.0</td><td>637.5</td><td>372.21</td><td>null</td><td>-122.5</td></tr><tr><td>2012-05-01</td><td>&quot;A Ashwin&quot;</td><td>180.0</td><td>95.0</td><td>210.0</td><td>485.0</td><td>326.82</td><td>null</td><td>55.0</td></tr><tr><td>2012-12-10</td><td>&quot;A Ashwin&quot;</td><td>170.0</td><td>95.0</td><td>220.0</td><td>485.0</td><td>324.79</td><td>223</td><td>10.0</td></tr><tr><td>2003-04-05</td><td>&quot;A Baltuonis&quot;</td><td>220.0</td><td>130.0</td><td>210.0</td><td>560.0</td><td>347.51</td><td>null</td><td>-50.0</td></tr><tr><td>2019-10-01</td><td>&quot;A Belousov&quot;</td><td>75.0</td><td>75.0</td><td>100.0</td><td>250.0</td><td>180.52</td><td>null</td><td>145.0</td></tr><tr><td>2003-04-05</td><td>&quot;A Brazdzius&quot;</td><td>290.0</td><td>180.0</td><td>280.0</td><td>750.0</td><td>446.7</td><td>null</td><td>-215.0</td></tr><tr><td>2004-04-03</td><td>&quot;A Brazdzius&quot;</td><td>300.0</td><td>182.5</td><td>305.0</td><td>787.5</td><td>465.71</td><td>364</td><td>-10.0</td></tr><tr><td>2022-06-17</td><td>&quot;A Dhanush&quot;</td><td>282.5</td><td>170.0</td><td>240.0</td><td>692.5</td><td>424.86</td><td>null</td><td>17.5</td></tr><tr><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td></tr><tr><td>2013-07-07</td><td>&quot;齋藤 誠一郎&quot;</td><td>230.0</td><td>192.5</td><td>242.5</td><td>665.0</td><td>399.85</td><td>3542</td><td>25.0</td></tr><tr><td>2015-09-20</td><td>&quot;齋藤 誠一郎&quot;</td><td>232.5</td><td>147.5</td><td>212.5</td><td>592.5</td><td>360.59</td><td>805</td><td>-2.5</td></tr><tr><td>2016-11-06</td><td>&quot;齋藤 誠一郎&quot;</td><td>225.0</td><td>145.0</td><td>215.0</td><td>585.0</td><td>352.0</td><td>413</td><td>7.5</td></tr><tr><td>2016-02-27</td><td>&quot;齋藤 颯太&quot;</td><td>220.0</td><td>162.5</td><td>245.0</td><td>627.5</td><td>358.81</td><td>null</td><td>5.0</td></tr><tr><td>2018-02-10</td><td>&quot;齋藤 颯太&quot;</td><td>260.0</td><td>182.5</td><td>272.5</td><td>715.0</td><td>406.16</td><td>0</td><td>-40.0</td></tr><tr><td>2018-02-10</td><td>&quot;齋藤 颯太&quot;</td><td>260.0</td><td>182.5</td><td>272.5</td><td>715.0</td><td>406.16</td><td>714</td><td>0.0</td></tr><tr><td>2017-02-25</td><td>&quot;齋藤 颯汰&quot;</td><td>251.0</td><td>175.0</td><td>252.5</td><td>678.5</td><td>385.54</td><td>null</td><td>9.0</td></tr><tr><td>2018-02-10</td><td>&quot;齋藤 駿弥&quot;</td><td>120.0</td><td>95.0</td><td>160.0</td><td>375.0</td><td>312.56</td><td>null</td><td>131.0</td></tr><tr><td>2018-08-12</td><td>&quot;齋藤 駿弥&quot;</td><td>130.0</td><td>95.0</td><td>160.0</td><td>385.0</td><td>317.26</td><td>183</td><td>-10.0</td></tr><tr><td>2018-08-12</td><td>&quot;齋藤 魁斗&quot;</td><td>192.5</td><td>122.5</td><td>215.0</td><td>530.0</td><td>361.12</td><td>null</td><td>-62.5</td></tr><tr><td>2018-02-10</td><td>&quot;齋藤 龍&quot;</td><td>230.0</td><td>140.0</td><td>240.0</td><td>610.0</td><td>407.17</td><td>null</td><td>-37.5</td></tr><tr><td>2019-02-09</td><td>&quot;齋藤 龍&quot;</td><td>230.0</td><td>135.0</td><td>260.0</td><td>625.0</td><td>450.87</td><td>364</td><td>0.0</td></tr></tbody></table></div>



## Modelling


```python
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import OrdinalEncoder, OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
```


```python
param_grid = {
    'regressor__bootstrap': [True],
    'regressor__max_depth': [80, 90, 100, 110],
    'regressor__max_features': [2, 3],
    'regressor__min_samples_leaf': [3, 4, 5],
    'regressor__min_samples_split': [8, 10, 12],
    'regressor__n_estimators': [100, 200, 300, 1000]
}

```


```python
fe_df.schema
```




    {'date': Date,
     'name': Utf8,
     'sex': Utf8,
     'place': Int32,
     'age': Float64,
     'age_class': Utf8,
     'bodyweight': Float64,
     'meet_country': Utf8,
     'equipment': Utf8,
     'squat': Float64,
     'bench': Float64,
     'deadlift': Float64,
     'total': Float64,
     'wilks': Float64,
     'meet_name': Utf8,
     'origin_country': Utf8,
     'days_since_last_competition': Int32,
     'squat_progress_per_day': Float64,
     'bench_progress_per_day': Float64,
     'deadlift_progress_per_day': Float64,
     'total_progress_per_day': Float64,
     'wilks_progress_per_day': Float64,
     'is_origin_country': Boolean,
     'date_as_ordinal': Int64}




```python
features = [
    "date_as_ordinal", "name", "sex", "age", "age_class", "bodyweight", "equipment", "total", "place", "time_since_last_comp", "squat_progress", "bench_progress", "deadlift_progress", "total_progress", "wilks_progress", "origin_country", "is_origin_country"
]
target = [
    "total"
]


# Preprocessing steps for numeric features
numeric_transformer = Pipeline([
    ('scaler', StandardScaler())
])

# Preprocessing steps for categorical features
categorical_transformer = Pipeline([
    ('encoder', OneHotEncoder())
])


# Preprocessing steps for label encoded features
ordinal_transformer = Pipeline([
    ('encoder', OrdinalEncoder())
])

# Combine preprocessing steps for all features
preprocessor = ColumnTransformer([
    ('numeric', numeric_transformer, ['age', 'bodyweight','time_since_last_comp', "squat_progress", "bench_progress", "deadlift_progress", "total_progress", "wilks_progress", 'date_as_ordinal', 'total']),
    ('categorical', categorical_transformer, ['sex', 'is_origin_country']),
    ('ordinal', ordinal_transformer, ['place', 'name', 'age_class', 'origin_country'])
])

# Create the pipeline with preprocessing steps and the regressor
pipeline = Pipeline([
    ('preprocessor', preprocessor),
    ('regressor', RandomForestRegressor())
])
```


```python
fe_df = data_prep_df.with_columns(
     (pl.col('date') - pl.col('date').shift(-1)).over('name').alias('time_since_last_comp').apply(lambda x: x.days).cast(pl.Int32)
).filter(pl.col("name") == "A. Belevskiy")
```


```python
fe_df.tail(2).to_dict(as_series=True)
```




    {'date': shape: (2,)
     Series: 'date' [date]
     [
     	2015-10-05
     	2015-10-05
     ],
     'name': shape: (2,)
     Series: 'name' [str]
     [
     	"A. Belevskiy"
     	"A. Belevskiy"
     ],
     'sex': shape: (2,)
     Series: 'sex' [str]
     [
     	"M"
     	"M"
     ],
     'place': shape: (2,)
     Series: 'place' [i32]
     [
     	1
     	4
     ],
     'age': shape: (2,)
     Series: 'age' [f64]
     [
     	16.5
     	16.5
     ],
     'age_class': shape: (2,)
     Series: 'age_class' [str]
     [
     	"16-17"
     	"16-17"
     ],
     'bodyweight': shape: (2,)
     Series: 'bodyweight' [f64]
     [
     	94.35
     	96.0
     ],
     'meet_country': shape: (2,)
     Series: 'meet_country' [str]
     [
     	"Kazakhstan"
     	"Kazakhstan"
     ],
     'equipment': shape: (2,)
     Series: 'equipment' [str]
     [
     	"Raw"
     	"Single-ply"
     ],
     'squat': shape: (2,)
     Series: 'squat' [f64]
     [
     	200.0
     	250.0
     ],
     'bench': shape: (2,)
     Series: 'bench' [f64]
     [
     	142.5
     	150.0
     ],
     'deadlift': shape: (2,)
     Series: 'deadlift' [f64]
     [
     	210.0
     	240.0
     ],
     'total': shape: (2,)
     Series: 'total' [f64]
     [
     	552.5
     	640.0
     ],
     'wilks': shape: (2,)
     Series: 'wilks' [f64]
     [
     	344.75
     	396.24
     ],
     'meet_name': shape: (2,)
     Series: 'meet_name' [str]
     [
     	"Kazakhstan Pow…
     	"Kazakhstan Pow…
     ],
     'origin_country': shape: (2,)
     Series: 'origin_country' [str]
     [
     	"Kazakhstan"
     	"Kazakhstan"
     ],
     'time_since_last_comp': shape: (2,)
     Series: 'time_since_last_comp' [i32]
     [
     	0
     	null
     ]}




```python
fe_df.filter(pl.col("name") == 'A. Belevskiy')
```




<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
}
</style>
<small>shape: (4, 24)</small><table border="1" class="dataframe"><thead><tr><th>date</th><th>name</th><th>sex</th><th>place</th><th>age</th><th>age_class</th><th>bodyweight</th><th>meet_country</th><th>equipment</th><th>squat</th><th>bench</th><th>deadlift</th><th>total</th><th>wilks</th><th>meet_name</th><th>origin_country</th><th>time_since_last_comp</th><th>squat_progress</th><th>bench_progress</th><th>deadlift_progress</th><th>total_progress</th><th>wilks_progress</th><th>is_origin_country</th><th>date_as_ordinal</th></tr><tr><td>date</td><td>str</td><td>str</td><td>i32</td><td>f64</td><td>str</td><td>f64</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>str</td><td>str</td><td>i32</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>bool</td><td>i64</td></tr></thead><tbody><tr><td>2016-07-04</td><td>&quot;A. Belevskiy&quot;</td><td>&quot;M&quot;</td><td>1</td><td>17.5</td><td>&quot;18-19&quot;</td><td>97.2</td><td>&quot;Kazakhstan&quot;</td><td>&quot;Single-ply&quot;</td><td>235.0</td><td>172.5</td><td>235.0</td><td>642.5</td><td>395.64</td><td>&quot;Kazakhstan Sub…</td><td>&quot;Kazakhstan&quot;</td><td>93</td><td>4.455852</td><td>5.092402</td><td>1.909651</td><td>11.457906</td><td>5.135688</td><td>true</td><td>736149</td></tr><tr><td>2016-04-02</td><td>&quot;A. Belevskiy&quot;</td><td>&quot;M&quot;</td><td>1</td><td>17.5</td><td>&quot;18-19&quot;</td><td>98.65</td><td>&quot;Kazakhstan&quot;</td><td>&quot;Raw&quot;</td><td>212.5</td><td>147.5</td><td>215.0</td><td>575.0</td><td>351.88</td><td>&quot;Kazakhstan Cla…</td><td>&quot;Kazakhstan&quot;</td><td>31</td><td>1.909651</td><td>2.121834</td><td>1.697467</td><td>5.728953</td><td>3.714059</td><td>true</td><td>736056</td></tr><tr><td>2016-03-02</td><td>&quot;A. Belevskiy&quot;</td><td>&quot;M&quot;</td><td>1</td><td>17.5</td><td>&quot;18-19&quot;</td><td>97.45</td><td>&quot;Kazakhstan&quot;</td><td>&quot;Single-ply&quot;</td><td>235.0</td><td>160.0</td><td>230.0</td><td>625.0</td><td>384.45</td><td>&quot;Kazakhstan Pow…</td><td>&quot;Kazakhstan&quot;</td><td>149</td><td>-9.178645</td><td>-5.099247</td><td>-6.119097</td><td>-20.396988</td><td>-13.286598</td><td>true</td><td>736025</td></tr><tr><td>2015-10-05</td><td>&quot;A. Belevskiy&quot;</td><td>&quot;M&quot;</td><td>1</td><td>16.5</td><td>&quot;16-17&quot;</td><td>94.35</td><td>&quot;Kazakhstan&quot;</td><td>&quot;Raw&quot;</td><td>200.0</td><td>142.5</td><td>210.0</td><td>552.5</td><td>344.75</td><td>&quot;Kazakhstan Pow…</td><td>&quot;Kazakhstan&quot;</td><td>0</td><td>0.0</td><td>0.0</td><td>0.0</td><td>0.0</td><td>0.0</td><td>true</td><td>735876</td></tr></tbody></table></div>




```python
X_train, X_test, y_train, y_test = train_test_split(fe_df[features], fe_df[target], test_size=0.2, random_state=42)
```


```python
pipeline.fit(X_train.to_pandas(), y_train.to_pandas())
```

    /Users/namtonthat/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/pipeline.py:346: DataConversionWarning: A column-vector y was passed when a 1d array was expected. Please change the shape of y to (n_samples,), for example using ravel().
      self._final_estimator.fit(Xt, y, **fit_params_last_step)





    Pipeline(steps=[('preprocessor',
                     ColumnTransformer(transformers=[('numeric',
                                                      Pipeline(steps=[('scaler',
                                                                       StandardScaler())]),
                                                      ['age', 'bodyweight',
                                                       'time_since_last_comp',
                                                       'squat_progress',
                                                       'bench_progress',
                                                       'deadlift_progress',
                                                       'total_progress',
                                                       'wilks_progress',
                                                       'date_as_ordinal',
                                                       'total']),
                                                     ('categorical',
                                                      Pipeline(steps=[('encoder',
                                                                       OneHotEncoder())]),
                                                      ['sex', 'is_origin_country']),
                                                     ('ordinal',
                                                      Pipeline(steps=[('encoder',
                                                                       OrdinalEncoder())]),
                                                      ['place', 'name', 'age_class',
                                                       'origin_country'])])),
                    ('regressor', RandomForestRegressor())])




```python
# Initialize the grid search model
grid_search = GridSearchCV(estimator = pipeline, param_grid = param_grid, 
                           cv = 3, n_jobs = -1, verbose = 2)

# Fit the grid search model
grid_search.fit(X_train.to_pandas(), y_train.to_pandas())

# Get the best parameters
best_params = grid_search.best_params_

# Train the model using the best parameters
best_model = RandomForestRegressor(**best_params)
best_model.fit(X_train.to_pandas(), y_train.to_pandas())

# Make predictions
y_pred = best_model.predict(X_test.to_pandas())

```


```python
print(f"Mean Squared Error: {mse}")
```
