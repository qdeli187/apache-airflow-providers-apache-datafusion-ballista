# apache-airflow-providers-apache-datafusion-ballista
🍃 Run Apache Datafusion Ballista workflows within Airflow 

## ⚠️ Disclamer

This project is not ready yet ! for a functionnable version for local ballista workflow we need to wait for airflow 3.1.1 (waiting for this [PR](https://github.com/apache/airflow/pull/56660) to go live and test it)

## ✨ Features

- [ ] Local Executor
- [ ] Ballista Connection
- [ ] Remote Execution

## 🕹️ Usage

> Not ready yet and subject to change (no installable version but you can clone this repo and create a dag)

add this dependency to your airflow instance

```bash
pip install apache-airflow-providers-apache-datafusion-ballista
```

Create a Dag

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

