elasticdump \
  --input=https://Demo:Demo@1234@search-bie-demo-sxc42bjy3ywvd5s3omc3w2udny.ap-northeast-1.es.amazonaws.com/user_behavior \
  --output "s3://tx-example-data/opensearch/bie/user_behaviour.json" \
  --fileSize=1mb

elasticdump \
  --input=https://Demo:Demo@1234@search-bie-demo-sxc42bjy3ywvd5s3omc3w2udny.ap-northeast-1.es.amazonaws.com/user_behavior \
  --output=/home/ec2-user/temp/user_behaviour.json \
  --fileSize=1mb

--output=/data/my_index.json \
  --fileSize=10mb