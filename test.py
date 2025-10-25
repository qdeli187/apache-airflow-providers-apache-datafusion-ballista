from src.decorators.ballista import ballista_task
from airflow.decorators import dag , task
from datetime import datetime


@dag(schedule=None, start_date=datetime.now())
def test_dag():
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
