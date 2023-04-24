# dq_measures_python
Python Implementation of Data Quality Measures for Databricks

This is a Python Library for the maintenance and processing of Data Quality (DQ) Measures with distributed computing framework using Databricks.  Measures and analytic files may be independently run within Notebooks, allowing them to be grouped into parallel processes based on state, data dependency, time interval, or any custom business rules.  Each process can be calibrated to optimally meet demand and deliverables. Custom Python libraries will be created to facilitate consistent management and execution of processes as well as simplify the creation of new analyses. This design is ideal for imposing best practices amongst distributed services which are appropriately granted resources and permit focus on test-driven development.


- [ ] Create or modify a Runner Class
- [ ] Create or modify a Runner Manifest
- [ ] New modules are added to the Registry and Reverse Lookup
- [ ] Process the reverse lookup and all manifest files
- [ ] Encapsulating the _Thresholds_ File
- [ ] Increment the library version number(s)
- [ ] Build the library
- [ ] Upload the WHL file to the Databricks environment
- [ ] Deploy the library to the Databricks cluster

# Creating a New Measure

To author and

```python
from dqm.DQM_Metadata import DQM_Metadata
from dqm.DQMeasures import DQMeasures

class Runner_n():
```

## Create or modify a Runner Class
Measure semantic processing is implemented within a Runner class object. Measures methods may be re-used driven by parameters in the manifest or distinct functions may be implemented.
```python
    def my_measure(spark, dqm: DQMeasures, measure_id,  x) :

        z = f"""
                select
                     '{dqm.state}' as submtg_state_cd
                    ,'{measure_id}' as measure_id
                    ,'<series>' as submodule
                    , ... as numer
                    , ... as denom
                    , ... as mvalue
                    , ... as valid_value
                from
                    .
                    .
            """

        dqm.logger.debug(z)
        return spark.sql(z)
```
Measure functions must be included in the _v-table_ within the Runner class by name.
```python
    v_table = { '<callback>': my_measure }
```

## Create or modify a Runner Manifest
The runner manifest includes the metadata and parameters ( _if any_ ).  The manifest must be compiled with the process of building the distributable library.
```python
from pandas import pandas as pd
from pandas import DataFrame

run_n =[
    .
    .
    ['<series>', '<callback>', '<measure_id>', [param 1], .. [parameter n] ],
    .
    .
]


df = DataFrame(run_105, columns=['series', 'cb', 'measure_id', ... ])
df.to_pickle('./run_n.pkl')
```
## New modules are added to the Registry and Reverse Lookup
### Registry
Single instances of each _runner_ are contained within _Module_ for dispatching measures.
```python
.
.
from dqm.submodules import Runner_n as n
    .
    .
class Module():

    def __init__(self):
            .
            .
        self.run<series> = r<series>.Runner_n
            .
            .
        self.runners = {
                .
                .
            '<series>': self.run<series>,
                .
                .
        }
```

### Reverse Lookup
The reverse lookup compiles all of the _runner manifests_ and links individual measures by their ID.  The _reverse lookup_ must be compiled with the process of building the distributable library.
```python
    .
    .
series = [
    '901', '902', '903', '904', '905', '906', ...
    '201', '202', '204', '205', '206', ...
    '801', ...
    '101', '102', '103', '104', '105', '106', '107', ...
    '701', '702', '703', '704', ...
    '501', '502', '503', '504', ...
    '601', '602', ...
    ]
    .
    .
```

## Encapsulating the _Thresholds_ File


## Process the thresholds file

Copy the latest thresholds xlsx file produced by the researchers from SharePoint to the ```dqm\cfg``` folder. Delete any previous versions or an assertion will fail.

from within the ```dq_measures_python``` folder:

- > ```python .\dqm\thresholds.py```

## Process the reverse lookup and all manifest files

from within the ```dqm\batch``` folder:

- > ```python .\reverse_lookup.py```

## Increment the library version number(s)

The library version is included the source code. It can be updated in ```_init_()``` method of the DQMeasures module.
When running or deploying code in development, version should be set through the `VERSION` variable in your environment.

### Examples

> .env
```bash
export VERSION=2.6.10
```
* note, if the environment variable hasn't been picked up after you edited this file, try restarting the terminal or running `source .env` from the same directory.

> DQMeasures.py ( ~ line 96 )
```python
    self.version = '2.6.10' # internal library version
```

> DQMeasures.py ( ~ line 99 )
```python
    self.specvrsn = 'V2.6'  # specification version
```
> setup.py ( ~ line 8 )
```python
    version="2.6.10", # deployed library version
```

## Update VAL job metdata
> from the ```.\databricks\``` folder:
run ```.\reset_all_jobs_VAL.bat```

# Workflow

For automated and manual workflows, see [workflow.md](workflow.md).
