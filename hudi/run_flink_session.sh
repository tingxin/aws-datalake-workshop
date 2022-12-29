checkpoints=s3://starrocks/log/flink/
flink-yarn-session -jm 1024 -tm 2048 -s 4 \
 -D state.backend=rocksdb \
-D state.checkpoint-storage=filesystem \
-D state.checkpoints.dir=${checkpoints} \
-D execution.checkpointing.interval=60000 \
-D state.checkpoints.num-retained=5 \
-D execution.checkpointing.mode=EXACTLY_ONCE \
-D execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION \
-D state.backend.incremental=true \
-D execution.checkpointing.max-concurrent-checkpoints=1 \
-D rest.flamegraph.enabled=true \
-d \
-t /etc/hive/conf/hive-site.xml