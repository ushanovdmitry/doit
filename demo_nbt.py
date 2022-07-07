from doit import DAG, DictBackend, delayed
from doit.artifact import InMemoryArtifact
import datetime

Table = InMemoryArtifact


def score_f_us(target: Table):
    target.put_data(f"Score_F US calculated @ {datetime.date.today()}")


def score_m_us(target: Table):
    target.put_data(f"Score_M US calculated @ {datetime.date.today()}")


def score_f_eu(target: Table):
    target.put_data(f"Score_F EU calculated @ {datetime.date.today()}")


def score_m_eu(target: Table):
    target.put_data(f"Score_M EU calculated @ {datetime.date.today()}")


def total_score_f_us(target, scored_f_us, scored_m_us,):
    target.put_data(f"Total score F US: Combine data from {scored_f_us} and {scored_m_us}")


def total_score_m_us(target, scored_m_us,):
    target.put_data(f"Total score M US: Use data from {scored_m_us}")


def total_score_f_eu(target, scored):
    target.put_data(f"Total score F EU: Use data from {scored}")


def total_score_m_eu(target, scored):
    target.put_data(f"Total score M EU: Use data from {scored}")


def signal_factiva_us(target, factiva_ts, ):
    target.put_data(f"Calculate F US Signal: use data from {factiva_ts}")


def signal_moreover_us(target, factiva_ts, moreover_ts):
    target.put_data(f"Calculate F US Signal: use data from {factiva_ts} and {moreover_ts}")


def signal_moreover_eu(target, moreover_us):
    target.put_data(f"Calculate M EU Signal: use data from {moreover_us} and something else...")


def main():
    dag = DAG("EOD", )

    dag.py_task("Score F US", delayed(score_f_us)(target=Table("factiva_us.score").tar))
    dag.py_task("Score M US", delayed(score_m_us)(target=Table("moreover_us.score").tar))
    dag.py_task("Score F EU", delayed(score_f_eu)(target=Table("factiva_eu.score").tar))
    t4 = dag.py_task("Score M EU", delayed(score_f_eu)(target=Table("moreover_eu.score").tar))

    dag.py_task("Total Score F US", delayed(total_score_f_us)(
        target=Table("factiva_us.total_score").tar,
        scored_f_us=Table("factiva_us.score").dep,
        scored_m_us=Table("moreover_us.score").dep
    ))

    dag.py_task("Total Score M US", delayed(total_score_m_us)(
        target=Table("moreover_us.total_score").tar,
        scored_m_us=Table("moreover_us.score").dep
    ))

    dag.py_task("Calculate Signal F US", delayed(signal_factiva_us)(
        target=Table("factiva_us.signal_info").tar,
        factiva_ts=Table("factiva_us.total_score").dep
    ))

    dag.py_task("Calculate Signal M US", delayed(signal_moreover_us)(
        target=Table("moreover_us.signal_info").tar,
        factiva_ts=Table("factiva_us.total_score").dep,
        moreover_ts=Table("moreover_us.total_score").dep,
    ))

    dag.py_task("Calculate Signal M EU", delayed(signal_moreover_eu)(
        target=Table("moreover_eu.signal_info").tar,
        moreover_us=Table("moreover_us.total_score").dep
    ), depends_on_tasks=[t4, ])

    dag.render_online('https://dreampuf.github.io/GraphvizOnline/#')

    backend = DictBackend(dag.dag_name, ".eod.json")
    dag.run(backend)
    dag.run(backend)


if __name__ == '__main__':
    main()
