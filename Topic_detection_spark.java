import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import scala.Tuple2;
public class Topic_detection_spark {
	
public static void main(String[] args) {

SparkConf conf = new SparkConf().setAppName("word count").setMaster("local");
JavaSparkContext sc = new JavaSparkContext(conf);

//importer les fichiers
JavaRDD<String> speech_file= sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/wordcount/input/speech.txt");
JavaRDD<String> economic_file= sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/topicDetection/topics/economic.txt");
JavaRDD<String> politics_file= sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/topicDetection/topics/politics.txt");
JavaRDD<String> social_file= sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/topicDetection/topics/social.txt");


// On stocke les mots du trois fichiers dans des arrayList
List<String> str_ls_economic= new ArrayList<String>();
List<String> str_ls_politics= new ArrayList<String>();
List<String> str_ls_social= new ArrayList<String>();
// les mots sont stocké en lines alors on les stocke dans des ArrayList pour la comparaison après
for(String line:economic_file.collect()){ str_ls_economic.add(line); }
for(String line:politics_file.collect()){ str_ls_politics.add(line); }
for(String line:social_file.collect()){ str_ls_social.add(line); }


// On supprime les caracteres spéciaux dans le text et on le divise en mots
JavaRDD<String> words = speech_file.flatMap(s ->  Arrays.asList(s.replaceAll("[.,-:;\"]", "").split(" ")));

// On fait une transformation (clé, valeur) et la clé peut prendre l'un des valeur: economic, politic, social
JavaPairRDD<String, Integer> topics_count= words.mapToPair( word -> {
	
	for(String word_eco:str_ls_economic){
	    if(word.equalsIgnoreCase(word_eco)){ return new Tuple2<>("economic", 1); }
	}
	
	for(String word_pol:str_ls_politics){
	    if(word.equalsIgnoreCase(word_pol)){ return new Tuple2<>("politic", 1); }
	}
	
	for(String word_soc:str_ls_social){
	    if(word.equalsIgnoreCase(word_soc)){ return new Tuple2<>("social", 1); }
	}
	
	return new Tuple2<>("social", 0);
} );

// apres on applique le reducer on enregistre les resultats dans un fichier 
JavaPairRDD<String, Integer> result= topics_count.reduceByKey((a, b) -> a + b);

result.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/topicDetection/output/Result_spark");
	}
}
