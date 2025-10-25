# Datafusion Ballista Airflow Provider
🍃 Run Apache Datafusion Ballista workflows within Airflow 

## ⚠️ Disclamer

Ballista's python bindings seems to be going through a rough patch while the overall project seems to take a new turn for the better (having ballista as more of an engine extension to datafusion) This repo aims to work on having a nice airflow integration where devs can create a ballista connection and use the decorator similarly as they would with the apache spark decorator (with the same caveheat).

It might take some time for datafusion and ballista to have the synergy they hope so this repo will be on the starting block waiting for the amazing results to come afloat !  As you might have guessed this here is a very naive and blunt implementation of an airflow provider that only allows a single local worker (ie airflow worker = ballista standalone instance). When the bindings are stable and the "fusion" with datafusion is complete i will return here to work on 

**Do not try to use it in production**

## ✨ Features

- [x] Local Executor
- [ ] Ballista Connection
- [ ] Remote Execution

## 🕹️ Usage

Clone this repo and create a dag like the one below. At this stage we will not make a python package as it is a very dirty and naive implementation. Still you can play with the idea and see where it goes !

```python
from src.decorators.ballista import ballista_task
from airflow.decorators import dag , task
from datetime import datetime


@dag(schedule=None, start_date=datetime.now())
def test_dag():
    # if this provider is supported by airflow , you will have @task.ballista (but its not)
    @ballista_task
    def run(ctx):
        ctx.sql("SELECT 1").show()
        ctx.sql("SHOW TABLES").show()
        ctx.sql(
            "select name, value from information_schema.df_settings where name like 'ballista.job.name'"
        ).show()
    
    run()

if __name__ == "__main__":
    test_dag().test()
```

Test your dag locally
```
python /path/to/dag.py
```

