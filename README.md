Day01 
python3 /home/labuser/Desktop/Persistent_Folder/Utham/Day01/dellsupplier_datagen.py

Day02
python3 /home/labuser/Desktop/Persistent_Folder/Utham/Day02/supplier_dataingestion_pipeline.py > "LOG_FILE.txt" 2>&1;
python3 /home/labuser/Desktop/Persistent_Folder/Utham/Day02/supplier_dataread_pipeline.py > "LOG_FILE.txt" 2>&1;

Day03
python3 /home/labuser/Desktop/Persistent_Folder/Utham/Day03/python_datawarhouse_process.py

Day04
python3 /home/labuser/Desktop/Persistent_Folder/Utham/Day03/python_datawarhouse_process.py

Day05
python3 /home/labuser/Desktop/Persistent_Folder/Utham/Day05/dellsales_syntheticdata.py

python3 /home/labuser/Desktop/Persistent_Folder/Utham/Day05/Sales_Datamart_demo.py

===========================================

https://github.com/umathivagit/dell-data-architecture

https://console.neon.tech/


http://localhost:9001/browser


  
docker run --name postgres1 -e POSTGRES_PASSWORD=Root123$ -d postgres-pgvector
  
docker exec -it postgres1 psql -U postgres

set AWS_ACCESS_KEY_ID=SyVBytJJRPoVjhicfA41
set AWS_SECRET_ACCESS_KEY=A3jVjMoXinC4bDfQW8TFZsAyyzZKASPApGgnYSoK 
set AWS_REGION=us-east-1
set MINIO_ROOT_USER=myminioadmin
set MINIO_ROOT_PASSWORD=Root123$
set MINIO_REGION=us-east-1

export AWS_ACCESS_KEY_ID=SyVBytJJRPoVjhicfA41
export AWS_SECRET_ACCESS_KEY=A3jVjMoXinC4bDfQW8TFZsAyyzZKASPApGgnYSoK 
export AWS_REGION=us-east-1
export MINIO_ROOT_USER=myminioadmin
export MINIO_ROOT_PASSWORD=Root123$
export MINIO_REGION=us-east-1
