import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import scala.Tuple2;
public class Sentiment_analys_spark {
	
public static void main(String[] args) {

SparkConf conf = new SparkConf().setAppName("word count").setMaster("local");
JavaSparkContext sc = new JavaSparkContext(conf);

//importer les fichiers
JavaRDD<String> speech_file= sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/wordcount/input/speech.txt");
JavaRDD<String> negative_file= sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/tweets/input/negative-words.txt");
JavaRDD<String> positive_file= sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/tweets/input/positive-words.txt");
JavaRDD<String> stop_file= sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/tweets/input/stop-words.txt");


// On stocke les mots du trois fichiers dans des arrayList
List<String> str_ls_negative= new ArrayList<String>();
List<String> str_ls_positive= new ArrayList<String>();
List<String> str_ls_stop= new ArrayList<String>();
// les mots sont stocké en lines alors on les stocke dans des ArrayList pour la comparaison après
for(String line:negative_file.collect()){ str_ls_negative.add(line); }
for(String line:positive_file.collect()){ str_ls_positive.add(line); }
for(String line:stop_file.collect()){ str_ls_stop.add(line); }


// On supprime les caracteres spéciaux dans le text et on le divise en mots
JavaRDD<String> words = speech_file.flatMap(s ->  Arrays.asList(s.replaceAll("[.,-:;\"]", "").split(" ")));

// On fait une transformation (clé, valeur) et la clé peut prendre l'un des valeur: negative, positive, stop
JavaPairRDD<String, Integer> sentiments_count= words.mapToPair( word -> {
	
	for(String word_negatif:str_ls_negative){
	    if(word.equalsIgnoreCase(word_negatif)){ return new Tuple2<>("Negative", 1); }
	}
	
	for(String word_positive:str_ls_positive){
	    if(word.equalsIgnoreCase(word_positive)){ return new Tuple2<>("Positive", 1); }
	}
	
	for(String word_stop:str_ls_stop){
	    if(word.equalsIgnoreCase(word_stop)){ return new Tuple2<>("Stop", 1); }
	}
	
	return new Tuple2<>("Stop", 0);
} );

// apres on applique le reducer on enregistre les resultats dans un fichier 
JavaPairRDD<String, Integer> result= sentiments_count.reduceByKey((a, b) -> a + b);

result.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/tweets/output/Result_spark");
	}
}
