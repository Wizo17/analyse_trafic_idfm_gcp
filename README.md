# Analyse du trafic Ile de France Mobilité

python src/main.py "2025-02-21"
python src/load_raw_data.py "IDFM-gtfs_20250221_17" "2025-02-21"

pip freeze > requirements.txt

Linux
chmod +x setup_env.sh
./setup_env.sh


Windows
setup_env.bat




https://data.iledefrance-mobilites.fr/explore/dataset/offre-horaires-tc-gtfs-idfm/information/
8h - 13h - 17h
Download https://www.data.gouv.fr/fr/datasets/r/f9fff5b1-f9e4-4ec2-b8b3-8ad7005d869c
https://www.data.gouv.fr/fr/datasets/horaires-prevus-sur-les-lignes-de-transport-en-commun-dile-de-france-gtfs-datahub/


