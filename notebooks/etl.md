\\# ETL on \\OpenPowerlifting\\ Data \\\\\\python from IPython.display
import display, Markdown import polars as pl from datetime import
datetime as dt \\ read configs import sys from pathlib import Path
sys.path.append(str(Path().resolve().parent)) from steps import conf
\\\\\\ \\## Loading Data \\\\\\python s3\\file\\path =
f"https://{conf.bucket\\name}.s3.ap-southeast-2.amazonaws.com/{conf.parquet\\file}"
df = pl.read\\parquet(s3\\file\\path) df.head(5) \\\\\\
<span class="small">shape: (5, 41)</span>

| Name             | Sex | Event | Equipment | Age  | AgeClass | BirthYearClass | Division | BodyweightKg | WeightClassKg | Squat1Kg | Squat2Kg | Squat3Kg | Squat4Kg | Best3SquatKg | Bench1Kg | Bench2Kg | Bench3Kg | Bench4Kg | Best3BenchKg | Deadlift1Kg | Deadlift2Kg | Deadlift3Kg | Deadlift4Kg | Best3DeadliftKg | TotalKg | Place | Dots   | Wilks  | Glossbrenner | Goodlift | Tested | Country  | State | Federation | ParentFederation | Date         | MeetCountry | MeetState | MeetTown  | MeetName         |
|------------------|-----|-------|-----------|------|----------|----------------|----------|--------------|---------------|----------|----------|----------|----------|--------------|----------|----------|----------|----------|--------------|-------------|-------------|-------------|-------------|-----------------|---------|-------|--------|--------|--------------|----------|--------|----------|-------|------------|------------------|--------------|-------------|-----------|-----------|------------------|
| str              | str | str   | str       | f64  | str      | str            | str      | f64          | str           | f64      | f64      | f64      | f64      | f64          | f64      | f64      | f64      | f64      | f64          | f64         | f64         | f64         | f64         | f64             | f64     | str   | f64    | f64    | f64          | f64      | str    | str      | str   | str        | str              | str          | str         | str       | str       | str              |
| "Alona Vladi"    | "F" | "SBD" | "Raw"     | 33.0 | "24-34"  | "24-39"        | "O"      | 58.3         | "60"          | 75.0     | 80.0     | -90.0    | null     | 80.0         | 50.0     | 55.0     | 60.0     | null     | 60.0         | 95.0        | 105.0       | 107.5       | null        | 107.5           | 247.5   | "1"   | 279.44 | 282.18 | 249.42       | 57.1     | "Yes"  | "Russia" | null  | "GFP"      | null             | "2019-05-11" | "Russia"    | null      | "Bryansk" | "Open Tournamen… |
| "Galina Solovya… | "F" | "SBD" | "Raw"     | 43.0 | "40-44"  | "40-49"        | "M1"     | 73.1         | "75"          | 95.0     | 100.0    | 105.0    | null     | 105.0        | 62.5     | 67.5     | -72.5    | null     | 67.5         | 100.0       | 110.0       | -120.0      | null        | 110.0           | 282.5   | "1"   | 278.95 | 272.99 | 240.35       | 56.76    | "Yes"  | "Russia" | null  | "GFP"      | null             | "2019-05-11" | "Russia"    | null      | "Bryansk" | "Open Tournamen… |
| "Daniil Voronin… | "M" | "SBD" | "Raw"     | 15.5 | "16-17"  | "14-18"        | "T"      | 67.4         | "75"          | 85.0     | 90.0     | 100.0    | null     | 100.0        | 55.0     | 62.5     | -65.0    | null     | 62.5         | 90.0        | 100.0       | 105.0       | null        | 105.0           | 267.5   | "1"   | 206.4  | 206.49 | 200.45       | 41.24    | "Yes"  | "Russia" | null  | "GFP"      | null             | "2019-05-11" | "Russia"    | null      | "Bryansk" | "Open Tournamen… |
| "Aleksey Krasov… | "M" | "SBD" | "Raw"     | 35.0 | "35-39"  | "24-39"        | "O"      | 66.65        | "75"          | 125.0    | 132.0    | 137.5    | null     | 137.5        | 115.0    | 122.5    | -127.5   | null     | 122.5        | 150.0       | 165.0       | 170.0       | null        | 170.0           | 430.0   | "1"   | 334.49 | 334.94 | 325.32       | 66.68    | "Yes"  | "Russia" | null  | "GFP"      | null             | "2019-05-11" | "Russia"    | null      | "Bryansk" | "Open Tournamen… |
| "Margarita Ples… | "M" | "SBD" | "Raw"     | 26.5 | "24-34"  | "24-39"        | "O"      | 72.45        | "75"          | 80.0     | 85.0     | 90.0     | null     | 90.0         | 40.0     | 50.0     | -60.0    | null     | 50.0         | 112.5       | 120.0       | 125.0       | null        | 125.0           | 265.0   | "1"   | 194.46 | 193.55 | 187.29       | 39.34    | "Yes"  | "Russia" | null  | "GFP"      | null             | "2019-05-11" | "Russia"    | null      | "Bryansk" | "Open Tournamen… |

\\\\\\python cleansing\\data\\md = f""" \\# Cleansing data - Filter for
only particular columns from {conf.op\\cols} - Select events that are
IPF / Tested federations only (i.e. \\Tested\\ = "Yes") - Remove anyone
who has been disqualified (i.e. \\Place\\ = "DQ") - \\raw\\ equipment
events only (i.e. \\equipment\\ = \\Raw\\, single ply and wraps have
'competitive' advantage and are typically only for advanced lifters) -
Drop any rows that have missing values in the columns above. - Drop any
duplicates based on all columns. """
display(Markdown(cleansing\\data\\md)) \\\\\\ \\# Cleansing data -
Filter for only particular columns from \\'Date', 'Name', 'Sex',
'Place', 'Age', 'AgeClass', 'BodyweightKg', 'Event', 'MeetCountry',
'Equipment', 'Best3SquatKg', 'Best3BenchKg', 'Best3DeadliftKg',
'TotalKg', 'Wilks', 'Tested', 'Federation', 'MeetName'\\ - Select events
that are IPF / Tested federations only (i.e. \\Tested\\ = "Yes") -
Remove anyone who has been disqualified (i.e. \\Place\\ = "DQ") -
\\raw\\ equipment events only (i.e. \\equipment\\ = \\Raw\\, single ply
and wraps have 'competitive' advantage and are typically only for
advanced lifters) - Drop any rows that have missing values in the
columns above. - Drop any duplicates based on all columns. \\\\\\python
base\\df = df.select(conf.op\\cols) print(base\\df.shape)
base\\df.head(5) \\\\\\ (2907777, 18) <span class="small">shape: (5,
18)</span>

| Date         | Name             | Sex | Place | Age  | AgeClass | BodyweightKg | Event | MeetCountry | Equipment | Best3SquatKg | Best3BenchKg | Best3DeadliftKg | TotalKg | Wilks  | Tested | Federation | MeetName         |
|--------------|------------------|-----|-------|------|----------|--------------|-------|-------------|-----------|--------------|--------------|-----------------|---------|--------|--------|------------|------------------|
| str          | str              | str | str   | f64  | str      | f64          | str   | str         | str       | f64          | f64          | f64             | f64     | f64    | str    | str        | str              |
| "2019-05-11" | "Alona Vladi"    | "F" | "1"   | 33.0 | "24-34"  | 58.3         | "SBD" | "Russia"    | "Raw"     | 80.0         | 60.0         | 107.5           | 247.5   | 282.18 | "Yes"  | "GFP"      | "Open Tournamen… |
| "2019-05-11" | "Galina Solovya… | "F" | "1"   | 43.0 | "40-44"  | 73.1         | "SBD" | "Russia"    | "Raw"     | 105.0        | 67.5         | 110.0           | 282.5   | 272.99 | "Yes"  | "GFP"      | "Open Tournamen… |
| "2019-05-11" | "Daniil Voronin… | "M" | "1"   | 15.5 | "16-17"  | 67.4         | "SBD" | "Russia"    | "Raw"     | 100.0        | 62.5         | 105.0           | 267.5   | 206.49 | "Yes"  | "GFP"      | "Open Tournamen… |
| "2019-05-11" | "Aleksey Krasov… | "M" | "1"   | 35.0 | "35-39"  | 66.65        | "SBD" | "Russia"    | "Raw"     | 137.5        | 122.5        | 170.0           | 430.0   | 334.94 | "Yes"  | "GFP"      | "Open Tournamen… |
| "2019-05-11" | "Margarita Ples… | "M" | "1"   | 26.5 | "24-34"  | 72.45        | "SBD" | "Russia"    | "Raw"     | 90.0         | 50.0         | 125.0           | 265.0   | 193.55 | "Yes"  | "GFP"      | "Open Tournamen… |

\\\\\\python cleansed\\df = base\\df.filter( (pl.col("Event") == "SBD")
& (pl.col("Tested") == "Yes") & (pl.col('Place').apply(lambda x:
x.isnumeric(), return\\dtype = pl.Boolean)) & (pl.col("Equipment") ==
"Raw") ).drop\\nulls().unique().sort("Date",
descending=True).drop(\\"Tested", "Federation", "Event"\\)
print(cleansed\\df.shape) cleansed\\df.head(5) \\\\\\ (429429, 15)
<span class="small">shape: (5, 15)</span>

| Date         | Name             | Sex | Place | Age  | AgeClass | BodyweightKg | MeetCountry   | Equipment | Best3SquatKg | Best3BenchKg | Best3DeadliftKg | TotalKg | Wilks  | MeetName         |
|--------------|------------------|-----|-------|------|----------|--------------|---------------|-----------|--------------|--------------|-----------------|---------|--------|------------------|
| str          | str              | str | str   | f64  | str      | f64          | str           | str       | f64          | f64          | f64             | f64     | f64    | str              |
| "2023-05-07" | "Sandrine Genou… | "F" | "1"   | 35.0 | "35-39"  | 48.5         | "Switzerland" | "Raw"     | 100.0        | 50.0         | 117.5           | 267.5   | 351.58 | "Championnat Su… |
| "2023-05-07" | "Laura Delay"    | "F" | "2"   | 25.0 | "24-34"  | 50.3         | "Switzerland" | "Raw"     | 95.0         | 55.0         | 110.0           | 260.0   | 332.49 | "Championnat Su… |
| "2023-05-07" | "Selin Tabak"    | "F" | "1"   | 25.0 | "24-34"  | 56.4         | "Switzerland" | "Raw"     | 120.0        | 70.0         | 130.0           | 320.0   | 374.42 | "Championnat Su… |
| "2023-05-07" | "Célestine Nguy… | "F" | "1"   | 29.0 | "24-34"  | 62.0         | "Switzerland" | "Raw"     | 110.0        | 42.5         | 132.5           | 285.0   | 309.82 | "Championnat Su… |
| "2023-05-07" | "Daniela Niklau… | "F" | "1"   | 37.0 | "35-39"  | 66.5         | "Switzerland" | "Raw"     | 112.5        | 82.5         | 165.0           | 360.0   | 371.4  | "Championnat Su… |

\\\\\\python cleansed\\df.filter(pl.col("Name") == "John Paul
Cauchi").sort("Date", descending=True).head(5) \\\\\\
<span class="small">shape: (5, 15)</span>

| Date         | Name             | Sex | Place | Age  | AgeClass | BodyweightKg | MeetCountry | Equipment | Best3SquatKg | Best3BenchKg | Best3DeadliftKg | TotalKg | Wilks  | MeetName         |
|--------------|------------------|-----|-------|------|----------|--------------|-------------|-----------|--------------|--------------|-----------------|---------|--------|------------------|
| str          | str              | str | str   | f64  | str      | f64          | str         | str       | f64          | f64          | f64             | f64     | f64    | str              |
| "2022-11-05" | "John Paul Cauc… | "M" | "1"   | 29.5 | "24-34"  | 82.35        | "Australia" | "Raw"     | 265.0        | 137.5        | 290.0           | 692.5   | 464.42 | "Ballarat Open"  |
| "2021-04-11" | "John Paul Cauc… | "M" | "1"   | 28.0 | "24-34"  | 76.75        | "Australia" | "Raw"     | 260.0        | 138.0        | 290.0           | 688.0   | 482.56 | "JPS Open VII"   |
| "2020-09-26" | "John Paul Cauc… | "M" | "2"   | 27.5 | "24-34"  | 76.95        | "Australia" | "Raw"     | 256.0        | 133.0        | 306.0           | 695.0   | 486.62 | "Australian Pow… |
| "2019-08-22" | "John Paul Cauc… | "M" | "2"   | 26.5 | "24-34"  | 76.9         | "China"     | "Raw"     | 248.0        | 135.0        | 273.0           | 656.0   | 459.52 | "Asia Pacific O… |
| "2019-06-28" | "John Paul Cauc… | "M" | "1"   | 26.5 | "24-34"  | 76.85        | "Australia" | "Raw"     | 253.0        | 134.0        | 286.0           | 673.0   | 471.63 | "Australian Pow… |

\\# Data Preparation - Drop the \\Tested\\, \\Federation\\ and \\Event\\
columns as they are no longer needed. - Update column types: - \\Date\\
to \\Date\\ - \\Place\\ to \\Int64\\ - Collect data from 2000-01-01
onwards. - Rename columns from camel to snake case - Assume that a
powerlifter's country is from the first country that compete in.
\\\\\\python \\ find the first country that the powerlifter competed in
and assume that is their country of origin lifter\\country\\df =
cleansed\\df.groupby(\\"Name",
"Sex"\\).agg(pl.first("MeetCountry").alias("OriginCountry")) \\\\\\
\\\\\\python data\\prep\\df = cleansed\\df.sort(\\"Name", "Date"\\,
descending=\\False, True\\).join(lifter\\country\\df, on=\\"Name",
"Sex"\\).filter(pl.col("Date").gt("2000-01-01")).with\\columns(
pl.col("Date").str.strptime(pl.Date, fmt="%Y-%m-%d").alias("Date"),
pl.col("Place").cast(pl.Int32).alias("Place"), ).rename(
mapping=conf.op\\cols\\rename ).select( pl.all().map\\alias(lambda
col\\name: conf.camel\\to\\snake(col\\name)) )
print(data\\prep\\df.shape) data\\prep\\df.head(5) \\\\\\ (428601, 16)
<span class="small">shape: (5, 16)</span>

| date       | name             | sex | place | age  | age_class | bodyweight | meet_country | equipment | squat | bench | deadlift | total | wilks  | meet_name        | origin_country |
|------------|------------------|-----|-------|------|-----------|------------|--------------|-----------|-------|-------|----------|-------|--------|------------------|----------------|
| date       | str              | str | i32   | f64  | str       | f64        | str          | str       | f64   | f64   | f64      | f64   | f64    | str              | str            |
| 2017-12-04 | "A Ajeesha"      | "F" | 1     | 16.5 | "16-17"   | 71.1       | "India"      | "Raw"     | 112.5 | 55.0  | 132.5    | 300.0 | 295.29 | "Asian Classic … | "India"        |
| 2012-12-10 | "A Ashwin"       | "M" | 1     | 16.5 | "16-17"   | 82.55      | "India"      | "Raw"     | 170.0 | 95.0  | 220.0    | 485.0 | 324.79 | "Asian Classic … | "India"        |
| 2019-10-01 | "A Belousov"     | "M" | 8     | 17.5 | "18-19"   | 73.6       | "Kazakhstan" | "Raw"     | 75.0  | 75.0  | 100.0    | 250.0 | 180.52 | "Kazakhstan Cla… | "Kazakhstan"   |
| 2019-09-26 | "A K S Shri Ram… | "M" | 13    | 16.0 | "16-17"   | 78.2       | "India"      | "Raw"     | 117.5 | 50.0  | 150.0    | 317.5 | 219.95 | "Indian Classic… | "India"        |
| 2019-09-26 | "A Pradeep"      | "M" | 6     | 17.0 | "16-17"   | 80.7       | "India"      | "Raw"     | 150.0 | 97.5  | 170.0    | 417.5 | 283.48 | "Indian Classic… | "India"        |

\\## Feature Engineering \\### Creating a Primary Key - As we require to
track a lifter's progress over time, the problem we'll encounter is that
there is no primary key to define the dataset without a lifter's birth
date. - This could have been resolved if we had their birth date but in
lieu of this, we can derive their age assuming that everyone's birthday
is the start of each year. - The only problem that this would result in
is that we have two lifters of the same name with the same age (actually
likely) but not as like as the first scenario. A new dataframe is
created called \\primary\\key\\df\\ to generate this \\primary\\key\\
column \\### Columns added - Create a \\pot\\\\\\ (progress over time)
columns for \\wilks\\ and \\total\\ - Adds columns: -
\\time\\since\\last\\comp\\: identify how long it has been since their
last competition (in days) - \\home\\country\\: 1 if \\meet\\country\\
== \\origin\\country\\ else 0 - \\bodyweight\\change\\: change in
bodyweight since the last comp (in kg) - \\cumulative\\comps\\: running
total of the number of comopetitions completed - \\meet\\type\\:
categories each meet in \\local\\, \\national\\ or \\international\\ -
\\starting lifts\\: defines their starting lifts - Switches \\Date\\ to
ordinal as a new column \\date\\ass\\ordinal\\ \\\\\\python
primary\\key\\df = data\\prep\\df.with\\columns(
(pl.col("date").dt.strftime('%Y').cast(pl.Int32) -
pl.col("age")).cast(pl.Int32).cast(pl.Utf8).alias("year\\of\\birth")).with\\columns(
pl.concat\\str(\\pl.col('year\\of\\birth'), pl.lit("01-01")\\,
separator="-").str.strptime(pl.Date,
fmt="%Y-%m-%d").alias("date\\of\\birth")).with\\columns(
pl.concat\\str(\\pl.col('name').str.to\\lowercase().str.replace(' ',
pl.lit('-')), pl.col("date\\of\\birth")\\,
separator="-").alias('primary\\key')).unique(subset=\\"primary\\key",
"date", "meet\\name"\\).drop(\\"date\\of\\birth"\\)
primary\\key\\df.head(2) \\\\\\ <span class="small">shape: (2,
18)</span>

| date       | name        | sex | place | age  | age_class | bodyweight | meet_country | equipment | squat | bench | deadlift | total | wilks  | meet_name        | origin_country | year_of_birth | primary_key      |
|------------|-------------|-----|-------|------|-----------|------------|--------------|-----------|-------|-------|----------|-------|--------|------------------|----------------|---------------|------------------|
| date       | str         | str | i32   | f64  | str       | f64        | str          | str       | f64   | f64   | f64      | f64   | f64    | str              | str            | str           | str              |
| 2017-12-04 | "A Ajeesha" | "F" | 1     | 16.5 | "16-17"   | 71.1       | "India"      | "Raw"     | 112.5 | 55.0  | 132.5    | 300.0 | 295.29 | "Asian Classic … | "India"        | "2000"        | "a-ajeesha-2000… |
| 2012-12-10 | "A Ashwin"  | "M" | 1     | 16.5 | "16-17"   | 82.55      | "India"      | "Raw"     | 170.0 | 95.0  | 220.0    | 485.0 | 324.79 | "Asian Classic … | "India"        | "1995"        | "a-ashwin-1995-… |

\\\\\\python fe\\df = primary\\key\\df.with\\columns( (pl.col('date') -
pl.col('date').shift(-1)).over('primary\\key').alias('time\\since\\last\\comp').apply(lambda
x: x.days).cast(pl.Int32), (pl.col('bodyweight') -
pl.col('bodyweight').shift(-1)).over('primary\\key').alias('bodyweight\\change').cast(pl.Float64),
).sort( \\"name", "date"\\, descending=\\False, False\\ ).with\\columns(
(pl.col('time\\since\\last\\comp') /
365.25).alias('years\\since\\last\\comp'), (pl.col("meet\\country") ==
pl.col("origin\\country")).alias("is\\origin\\country"),
pl.col('date').apply(lambda x:
x.toordinal()).alias('date\\as\\ordinal'),
pl.col('name').cumcount().over('primary\\key').alias('cumulative\\comps'),
pl.when( pl.col("meet\\name").str.contains('national')
).then("national").otherwise( pl.when(
pl.col('meet\\name').str.contains('International\|World\|Commonwealth')
).then("international").otherwise("local")).alias('meet\\type'),
).filter( pl.col('time\\since\\last\\comp') != 0 ) \\ have to filter out
the time\\since\\last\\comp since there might be data entry handling
error fe\\df = fe\\df.with\\columns( ((pl.col('squat') -
pl.col('squat').shift(1)) /
pl.col('years\\since\\last\\comp')).over('primary\\key').alias(f'squat\\progress'),
((pl.col('bench') - pl.col('bench').shift(1)) /
pl.col('years\\since\\last\\comp')).over('primary\\key').alias(f'bench\\progress'),
((pl.col('deadlift') - pl.col('deadlift').shift(1)) /
pl.col('years\\since\\last\\comp')).over('primary\\key').alias(f'deadlift\\progress'),
((pl.col('total') - pl.col('total').shift(1)) /
pl.col('years\\since\\last\\comp')).over('primary\\key').alias(f'total\\progress'),
((pl.col('wilks') - pl.col('wilks').shift(1)) /
pl.col('years\\since\\last\\comp')).over('primary\\key').alias(f'wilks\\progress')
).drop\\nulls() \\\\\\ \\\\\\python
fe\\df.filter(pl.col("name").str.starts\\with('Dmitriy
Markov')).sort("date", descending=True).select(fe\\df.columns\\:25\\)
\\\\\\ <span class="small">shape: (4, 25)</span>

| date       | name             | sex | place | age  | age_class | bodyweight | meet_country | equipment | squat | bench | deadlift | total | wilks  | meet_name        | origin_country | year_of_birth | primary_key      | time_since_last_comp | bodyweight_change | years_since_last_comp | is_origin_country | date_as_ordinal | cumulative_comps | meet_type |
|------------|------------------|-----|-------|------|-----------|------------|--------------|-----------|-------|-------|----------|-------|--------|------------------|----------------|---------------|------------------|----------------------|-------------------|-----------------------|-------------------|-----------------|------------------|-----------|
| date       | str              | str | i32   | f64  | str       | f64        | str          | str       | f64   | f64   | f64      | f64   | f64    | str              | str            | str           | str              | i32                  | f64               | f64                   | bool              | i64             | u32              | str       |
| 2021-02-19 | "Dmitriy Markov… | "M" | 8     | 16.5 | "16-17"   | 78.3       | "Russia"     | "Raw"     | 127.5 | 77.5  | 147.5    | 352.5 | 243.99 | "Nizhny Novgoro… | "Russia"       | "2004"        | "dmitriy-markov… | 370                  | 8.95              | 1.013005              | true              | 737840          | 3                | "local"   |
| 2020-02-15 | "Dmitriy Markov… | "M" | 8     | 15.5 | "16-17"   | 69.35      | "Russia"     | "Raw"     | 120.0 | 65.0  | 137.5    | 322.5 | 243.42 | "Nizhny Novgoro… | "Russia"       | "2004"        | "dmitriy-markov… | 161                  | 3.7               | 0.440794              | true              | 737470          | 2                | "local"   |
| 2019-09-07 | "Dmitriy Markov… | "M" | 5     | 14.5 | "13-15"   | 65.65      | "Russia"     | "Raw"     | 105.0 | 55.0  | 120.0    | 280.0 | 220.82 | "Nizhny Novgoro… | "Russia"       | "2004"        | "dmitriy-markov… | 127                  | 0.65              | 0.347707              | true              | 737309          | 1                | "local"   |
| 2016-04-20 | "Dmitriy Markov… | "M" | 13    | 18.5 | "18-19"   | 79.9       | "Belarus"    | "Raw"     | 155.0 | 107.5 | 210.0    | 472.5 | 322.83 | "Belarus Studen… | "Belarus"      | "1997"        | "dmitriy-markov… | 468                  | -1.16             | 1.281314              | true              | 736074          | 1                | "local"   |

\\\\\\python fe\\df.sort(by=\\'bench\\progress'\\,
reverse=True).select(fe\\df.columns\\:30\\) \\\\\\
/var/folders/n1/f6ysz\\js0qg8xpr\\dzb94mjh0000gn/T/ipykernel\\83249/3986157213.py:1:
DeprecationWarning: \\reverse\\ is deprecated as an argument to
\\sort\\; use \\descending\\ instead.
fe\\df.sort(by=\\'bench\\progress'\\,
reverse=True).select(fe\\df.columns\\:30\\) <span class="small">shape:
(213364, 30)</span>

| date       | name             | sex | place | age  | age_class | bodyweight | meet_country | equipment | squat | bench | deadlift | total | wilks  | meet_name        | origin_country | year_of_birth | primary_key      | time_since_last_comp | bodyweight_change | years_since_last_comp | is_origin_country | date_as_ordinal | cumulative_comps | meet_type | squat_progress | bench_progress | deadlift_progress | total_progress | wilks_progress |
|------------|------------------|-----|-------|------|-----------|------------|--------------|-----------|-------|-------|----------|-------|--------|------------------|----------------|---------------|------------------|----------------------|-------------------|-----------------------|-------------------|-----------------|------------------|-----------|----------------|----------------|-------------------|----------------|----------------|
| date       | str              | str | i32   | f64  | str       | f64        | str          | str       | f64   | f64   | f64      | f64   | f64    | str              | str            | str           | str              | i32                  | f64               | f64                   | bool              | i64             | u32              | str       | f64            | f64            | f64               | f64            | f64            |
| 2022-03-05 | "Anna Kubek"     | "F" | 1     | 26.0 | "24-34"   | 65.15      | "USA"        | "Raw"     | 132.5 | 80.0  | 165.0    | 377.5 | 395.36 | "Utah State Cha… | "USA"          | "1996"        | "anna-kubek-199… | 1                    | -0.65             | 0.002738              | true              | 738219          | 1                | "local"   | 18262.5        | 9131.25        | 22828.125         | 50221.875      | 53268.06       |
| 2022-08-28 | "Matyas Gruszka… | "M" | 1     | 17.5 | "18-19"   | 64.8       | "Czechia"    | "Raw"     | 162.5 | 115.0 | 180.0    | 457.5 | 364.74 | "Mistrovství Mo… | "Poland"       | "2004"        | "matyas-gruszka… | 1                    | -1.2              | 0.002738              | false             | 738395          | 1                | "local"   | 11870.625      | 6391.875       | 5478.75           | 23741.25       | 20654.8875     |
| 2022-10-16 | "Jan Gazur"      | "M" | 1     | 17.5 | "18-19"   | 64.65      | "Czechia"    | "Raw"     | 160.0 | 105.0 | 200.0    | 465.0 | 371.45 | "Mistrovství Če… | "Czechia"      | "2004"        | "jan-gazur-2004… | 1                    | -0.3              | 0.002738              | true              | 738444          | 3                | "local"   | 18262.5        | 5478.75        | 25567.5           | 49308.75       | 39761.115      |
| 2022-10-16 | "Matyas Gruszka… | "M" | 2     | 17.5 | "18-19"   | 65.5       | "Czechia"    | "Raw"     | 150.0 | 112.5 | 195.0    | 457.5 | 361.49 | "Mistrovství Če… | "Poland"       | "2004"        | "matyas-gruszka… | 1                    | -0.25             | 0.002738              | false             | 738444          | 4                | "local"   | 7305.0         | 4565.625       | 23741.25          | 35611.875      | 28463.9325     |
| 2019-06-27 | "Piotr Jabłonsk… | "M" | 6     | 27.5 | "24-34"   | 79.25      | "Sweden"     | "Raw"     | 235.0 | 182.5 | 240.0    | 657.5 | 451.57 | "SM Klassisk St… | "Sweden"       | "1991"        | "piotr-jabłonsk… | 4                    | 6.0               | 0.010951              | true              | 737237          | 7                | "local"   | 5022.1875      | 4337.34375     | 456.5625          | 9816.09375     | 4845.04125     |
| 2022-03-23 | "Mason Mitchell… | "M" | 1     | 17.5 | "18-19"   | 92.71      | "USA"        | "Raw"     | 227.5 | 170.0 | 260.0    | 657.5 | 413.65 | "Teen Nationals… | "USA"          | "2004"        | "mason-mitchell… | 1                    | 3.37              | 0.002738              | true              | 738237          | 6                | "local"   | -2739.375      | 3652.5         | -2739.375         | -1826.25       | -3973.92       |
| 2022-10-16 | "Simon Barčiš"   | "M" | 1     | 17.5 | "18-19"   | 58.35      | "Czechia"    | "Raw"     | 105.0 | 80.0  | 162.5    | 347.5 | 304.13 | "Mistrovství Če… | "Czechia"      | "2004"        | "simon-barčiš-2… | 1                    | 0.3               | 0.002738              | true              | 738444          | 4                | "local"   | 5478.75        | 3652.5         | 15523.125         | 24654.375      | 21144.3225     |
| 2022-03-23 | "Clark Whitefie… | "M" | 2     | 17.0 | "16-17"   | 73.07      | "USA"        | "Raw"     | 220.0 | 150.0 | 225.0    | 595.0 | 431.88 | "Teen Nationals… | "Turkey"       | "2005"        | "clark-whitefie… | 1                    | -0.77             | 0.002738              | false             | 738237          | 2                | "local"   | 1826.25        | 2739.375       | 913.125           | 5478.75        | 5128.11        |
| 2015-05-22 | "Artur Zheltukh… | "M" | 3     | 23.5 | "24-34"   | 100.6      | "Russia"     | "Raw"     | 227.5 | 162.5 | 220.0    | 610.0 | 370.36 | "Irkutsk Powerl… | "Russia"       | "1991"        | "artur-zheltukh… | 5                    | 10.4              | 0.013689              | true              | 735740          | 2                | "local"   | 4565.625       | 2556.75        | 4017.75           | 11140.125      | 5743.191       |
| 2020-03-07 | "Aleksandr Skvo… | "M" | 1     | 16.0 | "16-17"   | 66.4       | "Russia"     | "Raw"     | 120.0 | 90.0  | 170.0    | 380.0 | 296.9  | "Central Distri… | "Russia"       | "2004"        | "aleksandr-skvo… | 8                    | 22.6              | 0.021903              | true              | 737491          | 1                | "local"   | 2511.09375     | 1940.390625    | 3081.796875       | 7533.28125     | 1863.231563    |
| 2017-10-12 | "Peter Schmidt"  | "M" | 1     | 42.5 | "40-44"   | 81.6       | "Germany"    | "Raw"     | 237.5 | 156.0 | 247.5    | 641.0 | 432.27 | "DM KDK Classic… | "Germany"      | "1974"        | "peter-schmidt-… | 5                    | 0.1               | 0.013689              | true              | 736614          | 7                | "local"   | 2739.375       | 1899.3         | 3469.875          | 8108.55        | 5448.7995      |
| 2017-08-05 | "Thomas Moviel"  | "M" | 1     | 39.5 | "35-39"   | 92.06      | "USA"        | "Raw"     | 225.0 | 162.5 | 247.5    | 635.0 | 400.85 | "Southeast Regi… | "USA"          | "1977"        | "thomas-moviel-… | 27                   | -5.24             | 0.073922              | true              | 736546          | 1                | "local"   | 2705.555556    | 1860.069444    | 2468.819444       | 7034.444444    | 4465.113611    |
| …          | …                | …   | …     | …    | …         | …          | …            | …         | …     | …     | …        | …     | …      | …                | …              | …             | …                | …                    | …                 | …                     | …                 | …               | …                | …         | …              | …              | …                 | …              | …              |
| 2014-04-24 | "Aleksey Pesilo… | "M" | 4     | 20.5 | "20-23"   | 88.15      | "Russia"     | "Raw"     | 190.0 | 122.5 | 205.0    | 517.5 | 333.95 | "Southern & Nor… | "Russia"       | "1993"        | "aleksey-pesilo… | 1                    | 5.8               | 0.002738              | true              | 735347          | 1                | "local"   | -9131.25       | -5478.75       | -7305.0           | -21915.0       | -19482.435     |
| 2017-05-20 | "Nikolay Kosare… | "M" | 2     | 15.5 | "16-17"   | 56.34      | "Russia"     | "Raw"     | 100.0 | 57.5  | 140.0    | 297.5 | 269.24 | "Irkutsk Oblast… | "Russia"       | "2001"        | "nikolay-kosare… | 1                    | -2.66             | 0.002738              | true              | 736469          | 2                | "local"   | -3652.5        | -5478.75       | -7305.0           | -16436.25      | -10015.155     |
| 2014-04-24 | "Svetlana Korol… | "F" | 1     | 20.5 | "20-23"   | 41.55      | "Russia"     | "Raw"     | 65.0  | 30.0  | 67.5     | 162.5 | 237.35 | "Southern & Nor… | "Russia"       | "1993"        | "svetlana-korol… | 1                    | -2.0              | 0.002738              | true              | 735347          | 2                | "local"   | -9131.25       | -5478.75       | -8218.125         | -22828.125     | -29815.3575    |
| 2017-05-20 | "Vadim Akhmetov… | "M" | 1     | 12.5 | "13-15"   | 53.0       | "Russia"     | "Raw"     | 95.0  | 55.0  | 117.5    | 267.5 | 257.35 | "Irkutsk Oblast… | "Russia"       | "2004"        | "vadim-akhmetov… | 1                    | -8.1              | 0.002738              | true              | 736469          | 4                | "local"   | -8218.125      | -5478.75       | -8218.125         | -21915.0       | -6362.655      |
| 2017-01-28 | "Nathaniel Marg… | "M" | 1     | 24.5 | "24-34"   | 51.15      | "USA"        | "Raw"     | 85.0  | 42.5  | 130.0    | 257.5 | 257.11 | "Northeast Iron… | "USA"          | "1992"        | "nathaniel-marg… | 6                    | -43.21            | 0.016427              | true              | 736357          | 3                | "local"   | -5935.3125     | -6239.6875     | -6391.875         | -18566.875     | -5713.7275     |
| 2014-04-24 | "Anastasiya Che… | "F" | 1     | 13.5 | "13-15"   | 41.35      | "Russia"     | "Raw"     | 45.0  | 35.0  | 65.0     | 145.0 | 212.41 | "Southern & Nor… | "Russia"       | "2000"        | "anastasiya-che… | 1                    | -4.85             | 0.002738              | true              | 735347          | 2                | "local"   | -6391.875      | -6391.875      | -4565.625         | -17349.375     | -18145.62      |
| 2017-05-20 | "Vitaliy Berezi… | "M" | 3     | 12.5 | "13-15"   | 61.15      | "Russia"     | "Raw"     | 80.0  | 60.0  | 92.5     | 232.5 | 194.92 | "Irkutsk Oblast… | "Russia"       | "2004"        | "vitaliy-berezi… | 1                    | -4.75             | 0.002738              | true              | 736469          | 1                | "local"   | -7305.0        | -6391.875      | -10044.375        | -23741.25      | -14233.7925    |
| 2014-04-24 | "Nikita Shushpa… | "M" | 6     | 17.5 | "18-19"   | 74.1       | "Russia"     | "Raw"     | 130.0 | 90.0  | 155.0    | 375.0 | 269.48 | "Southern & Nor… | "Russia"       | "1996"        | "nikita-shushpa… | 1                    | -3.95             | 0.002738              | true              | 735347          | 1                | "local"   | -7305.0        | -7305.0        | -5478.75          | -20088.75      | -10511.895     |
| 2014-04-24 | "Veronika Nikol… | "F" | 1     | 16.5 | "16-17"   | 58.85      | "Russia"     | "Raw"     | 95.0  | 50.0  | 105.0    | 250.0 | 282.95 | "Southern & Nor… | "Russia"       | "1997"        | "veronika-nikol… | 1                    | -0.8              | 0.002738              | true              | 735347          | 3                | "local"   | -9131.25       | -7305.0        | -4565.625         | -21001.875     | -22440.96      |
| 2014-04-24 | "Aleksandr Cher… | "M" | 3     | 22.5 | "20-23"   | 80.35      | "Russia"     | "Raw"     | 180.0 | 145.0 | 222.5    | 547.5 | 372.75 | "Southern & Nor… | "Russia"       | "1991"        | "aleksandr-cher… | 1                    | -6.45             | 0.002738              | true              | 735347          | 2                | "local"   | -18262.5       | -8218.125      | -14610.0          | -41090.625     | -20713.3275    |
| 2014-04-24 | "Yuliya Ivanova… | "F" | 1     | 15.5 | "16-17"   | 51.3       | "Russia"     | "Raw"     | 70.0  | 45.0  | 75.0     | 190.0 | 239.35 | "Southern & Nor… | "Russia"       | "1998"        | "yuliya-ivanova… | 1                    | -3.95             | 0.002738              | true              | 735347          | 2                | "local"   | -5478.75       | -8218.125      | -7305.0           | -21001.875     | -20070.4875    |
| 2014-04-24 | "Aleksey Blagod… | "M" | 7     | 22.0 | "20-23"   | 81.5       | "Russia"     | "Raw"     | 160.0 | 120.0 | 170.0    | 450.0 | 303.69 | "Southern & Nor… | "Russia"       | "1992"        | "aleksey-blagod… | 1                    | -1.5              | 0.002738              | true              | 735347          | 1                | "local"   | -17349.375     | -10044.375     | -29220.0          | -56613.75      | -36579.7875    |

\\# Visualisation \\\\\\python import altair as alt import seaborn as
sns numerical\\cols = \\ "age", "bodyweight", "bodyweight\\change",
"time\\since\\last\\comp", "cumulative\\comps", "total" \\
fe\\df\\numerical =
fe\\df.select(pl.col(numerical\\cols)).sample(5000).to\\pandas()
correlation\\df = fe\\df\\numerical.corr() \\\\\\ \\\\\\python
sns.heatmap(correlation\\df, annot=True) \\\\\\
\\\[png\\(etl\\files/etl\\18\\1.png) \\\\\\python \\ Create a scatter
plot for each feature against 'total' plots = \\
alt.Chart(fe\\df\\numerical).mark\\circle().encode(
x=alt.X(f'{feature}:Q', title=feature), y=alt.Y('total:Q',
title='Total'), tooltip=\\feature, 'total'\\ ).properties( width=200,
height=200, title=f'Total vs {feature}' ) for feature in numerical\\cols
\\ \\\\\\ \\\\\\python alt.hconcat(\\plots) \\\\\\ \\# Modelling
\\\\\\python from sklearn.model\\selection import GridSearchCV,
train\\test\\split from sklearn.metrics import mean\\squared\\error from
sklearn.preprocessing import OrdinalEncoder, OneHotEncoder,
StandardScaler from sklearn.pipeline import Pipeline from
sklearn.compose import ColumnTransformer from sklearn.linear\\model
import Ridge, LinearRegression \\\\\\ \\\\\\python param\\grid = {
'regressor\\\\bootstrap': \\True\\, 'regressor\\\\max\\depth': \\80, 90,
100, 110\\, 'regressor\\\\max\\features': \\2, 3\\,
'regressor\\\\min\\samples\\leaf': \\3, 4, 5\\,
'regressor\\\\min\\samples\\split': \\8, 10, 12\\,
'regressor\\\\n\\estimators': \\100, 200, 300, 1000\\ } \\\\\\
\\\\\\python features = \\ "date\\as\\ordinal", "sex", "age",
"age\\class", "bodyweight", "equipment", "place",
"time\\since\\last\\comp", "squat\\progress", "bench\\progress",
"deadlift\\progress", "total\\progress", "wilks\\progress",
"origin\\country", "is\\origin\\country", "meet\\type",
"cumulative\\comps","bodyweight\\change" \\ target = \\ "total" \\ \\
Preprocessing steps for numeric features numeric\\transformer =
Pipeline(\\ ('scaler', StandardScaler()) \\) \\ Preprocessing steps for
categorical features categorical\\transformer = Pipeline(\\ ('encoder',
OneHotEncoder()) \\) \\ Preprocessing steps for label encoded features
ordinal\\transformer = Pipeline(\\ ('encoder',
OrdinalEncoder(handle\\unknown='use\\encoded\\value',
unknown\\value=-1)) \\) \\ Combine preprocessing steps for all features
preprocessor = ColumnTransformer(\\ ('numeric', numeric\\transformer,
\\'age', 'bodyweight','time\\since\\last\\comp', "squat\\progress",
"bench\\progress", "deadlift\\progress", "total\\progress",
"wilks\\progress", 'date\\as\\ordinal', 'bodyweight\\change',
'cumulative\\comps'\\), ('categorical', categorical\\transformer,
\\'is\\origin\\country'\\), ('ordinal', ordinal\\transformer, \\'place',
'age\\class', 'origin\\country', 'meet\\type', 'sex'\\) \\) \\ Create
the pipeline with preprocessing steps and the regressor pipeline =
Pipeline(\\ ('preprocessor', preprocessor), ('regressor', Ridge()) \\)
\\\\\\ \\## Basic Linear Regression \\\\\\python X\\train, X\\test,
y\\train, y\\test = train\\test\\split(fe\\df\\features\\,
fe\\df\\target\\, test\\size=0.2, random\\state=42) \\ standard pipeline
prediction pipeline.fit(X\\train.to\\pandas(),
y\\train.to\\pandas().values.ravel()) y\\pred =
pipeline.predict(X\\test.to\\pandas()) mse =
mean\\squared\\error(y\\test.to\\pandas(), y\\pred) print(f"mse: {mse}")
\\\\\\ mse: 6979.037040358594 \\### Bayesian Optimization \\\\\\python
\\ Initialize the grid search model grid\\search =
GridSearchCV(estimator = pipeline, param\\grid = param\\grid, cv = 3,
scoring="neg\\mean\\squared\\error", verbose = 2) \\ Fit the grid search
model grid\\search.fit(X\\train.to\\pandas(),
y\\train.to\\pandas().values.ravel()) \\ Get the best parameters
best\\params = grid\\search.best\\params\\ \\ Train the model using the
best parameters best\\model = Ridge(\\\\best\\params)
best\\model.fit(X\\train.to\\pandas(),
y\\train.to\\pandas().values.ravel()) \\ Make predictions y\\pred =
best\\model.predict(X\\test.to\\pandas()) \\\\\\ Fitting 3 folds for
each of 288 candidates, totalling 864 fits
---------------------------------------------------------------------------
ValueError Traceback (most recent call last) Cell In\\257\\, line 6 2
grid\\search = GridSearchCV(estimator = pipeline, param\\grid =
param\\grid, 3 cv = 3, scoring="neg\\mean\\squared\\error", verbose = 2)
5 \\ Fit the grid search model ----\> 6
grid\\search.fit(X\\train.to\\pandas(),
y\\train.to\\pandas().values.ravel()) 8 \\ Get the best parameters 9
best\\params = grid\\search.best\\params\\ File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/utils/validation.py:63,
in
\\deprecate\\positional\\args..\\inner\\deprecate\\positional\\args..inner\\f(\\args,
\\\\kwargs) 61 extra\\args = len(args) - len(all\\args) 62 if
extra\\args \<= 0: ---\> 63 return f(\\args, \\\\kwargs) 65 \\
extra\\args \> 0 66 args\\msg = \\'{}={}'.format(name, arg) 67 for name,
arg in zip(kwonly\\args\\:extra\\args\\, 68 args\\-extra\\args:\\)\\
File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/model\\selection/\\search.py:841,
in BaseSearchCV.fit(self, X, y, groups, \\\\fit\\params) 835 results =
self.\\format\\results( 836 all\\candidate\\params, n\\splits, all\\out,
837 all\\more\\results) 839 return results --\> 841
self.\\run\\search(evaluate\\candidates) 843 \\ multimetric is
determined here because in the case of a callable 844 \\ self.scoring
the return type is only known after calling 845 first\\test\\score =
all\\out\\0\\\\'test\\scores'\\ File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/model\\selection/\\search.py:1296,
in GridSearchCV.\\run\\search(self, evaluate\\candidates) 1294 def
\\run\\search(self, evaluate\\candidates): 1295 """Search all candidates
in param\\grid""" -\> 1296
evaluate\\candidates(ParameterGrid(self.param\\grid)) File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/model\\selection/\\search.py:795,
in BaseSearchCV.fit..evaluate\\candidates(candidate\\params, cv,
more\\results) 790 if self.verbose \> 0: 791 print("Fitting {0} folds
for each of {1} candidates," 792 " totalling {2} fits".format( 793
n\\splits, n\\candidates, n\\candidates \\ n\\splits)) --\> 795 out =
parallel(delayed(\\fit\\and\\score)(clone(base\\estimator), 796 X, y,
797 train=train, test=test, 798 parameters=parameters, 799
split\\progress=( 800 split\\idx, 801 n\\splits), 802
candidate\\progress=( 803 cand\\idx, 804 n\\candidates), 805
\\\\fit\\and\\score\\kwargs) 806 for (cand\\idx, parameters), 807
(split\\idx, (train, test)) in product( 808
enumerate(candidate\\params), 809 enumerate(cv.split(X, y, groups))))
811 if len(out) \< 1: 812 raise ValueError('No fits were performed. '
813 'Was the CV iterator empty? ' 814 'Were there no candidates?') File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/joblib/parallel.py:1085,
in Parallel.\\\\call\\\\(self, iterable) 1076 try: 1077 \\ Only set
self.\\iterating to True if at least a batch 1078 \\ was dispatched. In
particular this covers the edge (...) 1082 \\ was very quick and its
callback already dispatched all the 1083 \\ remaining jobs. 1084
self.\\iterating = False -\> 1085 if
self.dispatch\\one\\batch(iterator): 1086 self.\\iterating =
self.\\original\\iterator is not None 1088 while
self.dispatch\\one\\batch(iterator): File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/joblib/parallel.py:901,
in Parallel.dispatch\\one\\batch(self, iterator) 899 return False 900
else: --\> 901 self.\\dispatch(tasks) 902 return True File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/joblib/parallel.py:819,
in Parallel.\\dispatch(self, batch) 817 with self.\\lock: 818 job\\idx =
len(self.\\jobs) --\> 819 job = self.\\backend.apply\\async(batch,
callback=cb) 820 \\ A job can complete so quickly than its callback is
821 \\ called before we get here, causing self.\\jobs to 822 \\ grow. To
ensure correct results ordering, .insert is 823 \\ used (rather than
.append) in the following line 824 self.\\jobs.insert(job\\idx, job)
File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/joblib/\\parallel\\backends.py:208,
in SequentialBackend.apply\\async(self, func, callback) 206 def
apply\\async(self, func, callback=None): 207 """Schedule a func to be
run""" --\> 208 result = ImmediateResult(func) 209 if callback: 210
callback(result) File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/joblib/\\parallel\\backends.py:597,
in ImmediateResult.\\\\init\\\\(self, batch) 594 def \\\\init\\\\(self,
batch): 595 \\ Don't delay the application, to avoid keeping the input
596 \\ arguments in memory --\> 597 self.results = batch() File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/joblib/parallel.py:288,
in BatchedCalls.\\\\call\\\\(self) 284 def \\\\call\\\\(self): 285 \\
Set the default nested backend to self.\\backend but do not set the 286
\\ change the default number of processes to -1 287 with
parallel\\backend(self.\\backend, n\\jobs=self.\\n\\jobs): --\> 288
return \\func(\\args, \\\\kwargs) 289 for func, args, kwargs in
self.items\\ File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/joblib/parallel.py:288,
in (.0) 284 def \\\\call\\\\(self): 285 \\ Set the default nested
backend to self.\\backend but do not set the 286 \\ change the default
number of processes to -1 287 with parallel\\backend(self.\\backend,
n\\jobs=self.\\n\\jobs): --\> 288 return \\func(\\args, \\\\kwargs) 289
for func, args, kwargs in self.items\\ File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/utils/fixes.py:222,
in \\FuncWrapper.\\\\call\\\\(self, \\args, \\\\kwargs) 220 def
\\\\call\\\\(self, \\args, \\\\kwargs): 221 with
config\\context(\\\\self.config): --\> 222 return self.function(\\args,
\\\\kwargs) File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/model\\selection/\\validation.py:586,
in \\fit\\and\\score(estimator, X, y, scorer, train, test, verbose,
parameters, fit\\params, return\\train\\score, return\\parameters,
return\\n\\test\\samples, return\\times, return\\estimator,
split\\progress, candidate\\progress, error\\score) 583 for k, v in
parameters.items(): 584 cloned\\parameters\\k\\ = clone(v, safe=False)
--\> 586 estimator = estimator.set\\params(\\\\cloned\\parameters) 588
start\\time = time.time() 590 X\\train, y\\train =
\\safe\\split(estimator, X, y, train) File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/pipeline.py:150,
in Pipeline.set\\params(self, \\\\kwargs) 139 def set\\params(self,
\\\\kwargs): 140 """Set the parameters of this estimator. 141 142 Valid
parameter keys can be listed with \\\\get\\params()\\\\. Note that (...)
148 self 149 """ --\> 150 self.\\set\\params('steps', \\\\kwargs) 151
return self File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/utils/metaestimators.py:54,
in \\BaseComposition.\\set\\params(self, attr, \\\\params) 52
self.\\replace\\estimator(attr, name, params.pop(name)) 53 \\ 3. Step
parameters and other initialisation arguments ---\> 54
super().set\\params(\\\\params) 55 return self File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/base.py:242,
in BaseEstimator.set\\params(self, \\\\params) 239 valid\\params\\key\\
= value 241 for key, sub\\params in nested\\params.items(): --\> 242
valid\\params\\key\\.set\\params(\\\\sub\\params) 244 return self File
~/Library/Caches/pypoetry/virtualenvs/powerlifting-ml-progress-4gf5U7T4-py3.11/lib/python3.11/site-packages/sklearn/base.py:230,
in BaseEstimator.set\\params(self, \\\\params) 228 key, delim, sub\\key
= key.partition('\\\\') 229 if key not in valid\\params: --\> 230 raise
ValueError('Invalid parameter %s for estimator %s. ' 231 'Check the list
of available parameters ' 232 'with \\estimator.get\\params().keys()\\.'
% 233 (key, self)) 235 if delim: 236 nested\\params\\key\\\\sub\\key\\ =
value ValueError: Invalid parameter bootstrap for estimator Ridge().
Check the list of available parameters with
\\estimator.get\\params().keys()\\. \\\\\\python \\ Evaluate the model
mse = mean\\squared\\error(y\\test.to\\pandas(), y\\pred.to\\pandas())
print(f"Mean Squared Error: {mse}") \\\\\\
