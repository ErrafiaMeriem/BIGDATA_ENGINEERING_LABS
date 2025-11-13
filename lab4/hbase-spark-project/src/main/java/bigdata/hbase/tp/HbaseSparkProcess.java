package main.java.bigdata.hbase.tp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaPairRDD;

public class HbaseSparkProcess {

    public void createHbaseTable() {
        // 1. Crée la configuration HBase
        Configuration config = HBaseConfiguration.create();

        // 2. Configuration Spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseTest")
                .setMaster("local[4]");  // 4 cores sur la machine locale
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 3. Indiquer la table HBase à lire
        config.set(TableInputFormat.INPUT_TABLE, "products");

        // 4. Créer un RDD HBase
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(
                config,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class
        );

        // 5. Compter le nombre d'enregistrements
        System.out.println("nombre d'enregistrements: " + hBaseRDD.count());

        // 6. Fermer le contexte Spark
        jsc.close();
    }

    public static void main(String[] args) {
        HbaseSparkProcess admin = new HbaseSparkProcess();
        admin.createHbaseTable();
    }
}
