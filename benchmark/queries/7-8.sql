SET sequila.prefer_interval_join TO true;
SET datafusion.execution.target_partitions=1;
SET sequila.interval_join_algorithm TO coitrees;
SET datafusion.optimizer.repartition_joins TO false;
SET datafusion.execution.coalesce_batches TO false;

CREATE EXTERNAL TABLE a
STORED AS PARQUET
LOCATION '/Users/mwiewior/research/git/openstack-bdg-runners/ansible/roles/gha_runner/files/databio/ex-anno/*parquet';

CREATE EXTERNAL TABLE b
STORED AS PARQUET
LOCATION '/Users/mwiewior/research/git/openstack-bdg-runners/ansible/roles/gha_runner/files/databio/ex-rna/*parquet';

select count(1) from a join b
                            on a.contig = b.contig
                                and a.pos_end > b.pos_start
                                and a.pos_start < b.pos_end;
