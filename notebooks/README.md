## Landing

- Rename columns from camel to snake case

## Raw
*Goal*: Cleanse data using the `conf.py` to define configs
- Drop any rows that have missing values in the selected columns
- Drop any duplicates based on selected columns
- Update column types:
    - `Date` to `Date`
    - `Place` to `Int32` (i.e. ignoring any places like `DQ`)

### Primary Key
- Generate primary key from the `name`, `year-of-birth` (taken via `groupby`) followed by `country` (taken as the first country) that they competed in.

*Reasoning*
- As we require to track a lifter's progress over time,  the problem we'll encounter is that there is no primary key to define the dataset without a lifter's birth date.
- This could have been resolved if we had their birth date but in lieu of this, we can use their year of birth as a close enough approximation
- The only problem that this would result in is that we have two lifters of the same name with the same age (actually likely) but not as  likely as the first scenario.


## Base
*Goal*: Build the feature engineering columns
- Filter for only particular columns from `conf.op_cols`
    - Select events that are IPF / Tested federations only (i.e. `Tested` = "Yes")
    - `raw` equipment events only (i.e. `equipment` = `Raw`, single ply and wraps have 'competitive' advantage and are typically only for advanced lifters)

#### Columns added
- Create a `pot_*` (progress over time) columns for `wilks` and `total`
- Adds columns:
  - `time_since_last_comp`: identify how long it has been since their last competition (in days)
  - `home_country`: 1 if `meet_country` == `origin_country` else 0
  - `bodyweight_change`: change in bodyweight since the last comp (in kg)
  - `cumulative_comps`: running total of the number of comopetitions completed
  - `meet_type`: categories each meet in `local`, `national` or `international`
  - `ipf_weight_class`: defines the weight class for the lifter the IPF standards for male and females.
- Switches `Date` to ordinal as a new column `date_ass_ordinal` Assume that a powerlifter's country is from the first country that compete in.
