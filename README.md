# PSPD - P2 - Kafka e Spark

## 1. Pre-requisitos
- [Spark](https://spark.apache.org/downloads.html)
- [Kafka](https://kafka.apache.org/downloads)
- Python
- [GNU/Netcat](https://wiki.archlinux.org/title/Network_tools#Netcat)

## 2. Utilização

### 2.1 Spark com Socket
#### 2.1.1 Inicie o NetCat<br>
```
$ nc -lp 9999               # Escuta na porta 9999 no localhost
$ nc -lp 9999 < $PATH_TO_FILE        # Outra opção enviando um arquivo
```
#### 2.1.1 Rode o Spark
```
$ cd $SPARK_HOME                                # Va para onde instalou o Spark
$ bin/spark-submit $PROJECT_HOME/wc_socket.py   # Submetendo o wc_socket.py no Spark 
                                                # Sendo $PROJECT_HOME o caminho do projeto
```

### 2.2 Spark com Kafka
#### 2.2.1 Inicie o Kafka
```
$ cd $KAFKA_HOME
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties
```
### 2.2.2 Crie os topicos
```
$ cd $KAFKA_HOME

# Criando o topico 'wc' no server 'localhost:9092
$ bin/kafka-topics.sh --create --topic wc --bootstrap-server localhost:9092

# Criando o topico 'statistics' no server 'localhost:9092'
$ bin/kafka-topics.sh --create --topic statistics --bootstrap-server localhost:9092
```

### 2.2.3 Rode o Spark
```
$ cd $SPARK_HOME

# Iniciando o job que ira realizar transformacao nas palavras
$ bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 $PROJECT_HOME/wc_kafka.py

# Iniciando o job de visualização das estatisticas
$ bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 $PROJECT_HOME/stats_kafka.py
```

### 2.2.4 Alimentação do Topico
```
$ bin/kafka-console-producer.sh --topic wc --bootstrap-server localhost:9092

# Ou com um arquivo
$ bin/kafka-console-producer.sh --topic wc --bootstrap-server localhost:9092 < $PATH_TO_FILE
```