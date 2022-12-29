copy oms_parcels
from 's3://open-tx-data/fltw/oms_parcels/' 
iam_role 'arn:aws:iam::515491257789:role/AdminRole'
FORMAT AS PARQUET


copy oms_shipments
from 's3://open-tx-data/fltw/oms_shipments/' 
iam_role 'arn:aws:iam::515491257789:role/AdminRole'
FORMAT AS PARQUET


copy cms_parcel
from 's3://open-tx-data/fltw/cms_parcel/' 
iam_role 'arn:aws:iam::515491257789:role/AdminRole'
FORMAT AS PARQUET


copy cms_bill_purchase_detail
from 's3://open-tx-data/fltw/cms_bill_purchase_detail/' 
iam_role 'arn:aws:iam::515491257789:role/AdminRole'
FORMAT AS PARQUET

copy tms_parcel
from 's3://open-tx-data/fltw/tms_parcel/' 
iam_role 'arn:aws:iam::515491257789:role/AdminRole'
FORMAT AS PARQUET
