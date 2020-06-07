package pg1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;


public class Sentiment_analys_MR {
	
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException  {
		 
		Configuration	conf	= new	Configuration();	
		Job job = Job.getInstance(conf,"Sentiment_analys_MR");
		job.setJarByClass(Sentiment_analys_MR.class);
		job.setOutputKeyClass(Object.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(sentiment_analys_Mapper.class);
		job.setReducerClass(sentiment_analys_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)	?	0	:	1);	;
		}

public static class sentiment_analys_Mapper extends Mapper<Object, Text, Text, IntWritable>{
	
	// les variables word et one seront la sortie de la methode map(), word comme clé et one comme valeur
	private	Text word= new Text();
	private	final IntWritable one= new IntWritable(1);					
	
	 // on définit les chemins des trois fichiers
	 Path negative_words_path=new Path("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/tweets/input/negative-words.txt");
	 Path positive_words_path=new Path("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/tweets/input/positive-words.txt");
	 Path stop_words_path=new Path("hdfs://quickstart.cloudera:8020/user/cloudera/ateliers/atelier2/tweets/input/stop-words.txt");
	 
	 // les listes qui vont stocker les mots des fichier (negative_word, positive_words et stop_words)
	 List<String> str_negative_words= new ArrayList<String>();
	 List<String> str_positive_words= new ArrayList<String>();
	 List<String> str_stop_words= new ArrayList<String>();
	
	 
	// on redéfinir la methode setup() pour exécuter le code qui doit etre excuter une seul fois
	// la methode setup() est excuté une seul fois 
	public void setup(Context context) throws IOException{
			// On ajoute cette configuration pour pouvoir lire les fichiers stockés dans hdfs
		    Configuration conf = new Configuration();
		    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		    FileSystem fs = FileSystem.get(conf);
		    
		    BufferedReader br_negative_words= new BufferedReader(new InputStreamReader(fs.open(negative_words_path)));
		    BufferedReader br_positive_words= new BufferedReader(new InputStreamReader(fs.open(positive_words_path)));
		    BufferedReader br_stop_words= new BufferedReader(new InputStreamReader(fs.open(stop_words_path)));
		    
		    
		    // On stocke les mots de chaque topic dans un ArrayList pour faire la comparaison après
		    String line;
		    line= br_negative_words.readLine();
		    while (line != null){
		    	str_negative_words.add(line);
		        line=br_negative_words.readLine();
		    }
		    line= br_positive_words.readLine();
		    while (line != null){
		    	str_positive_words.add(line);
		        line=br_positive_words.readLine();
		    }
		    line= br_stop_words.readLine();
		    while (line != null){
		    	str_stop_words.add(line);
		        line=br_stop_words.readLine();
		    }
}
	
	// la methode map() qui prend comme entré la ligne du text speetch et comme sortie (topic, 1) [ topic == negative || positive || stop ]
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		// on utilise la methode split() qui nous permet de casser un string en mots
		String line = value.toString();
		List<String> speechWords= Arrays.asList(line.replaceAll("[.,-:;\"]", "").split(" "));
		
		for(String speechWord: speechWords){
			
			// Maintenant on fait la comparaison et si on trouve un mot negatif on declare comme sortie ("Negative", 1)
			// sinon on passe à l'autre boucle
			for(String str: str_negative_words ){
				if(speechWord.equalsIgnoreCase(str)){
					word.set("Negative");
					context.write(word, one);
				}
			}
			for(String str: str_positive_words ){
				if(speechWord.equalsIgnoreCase(str)){
					word.set("Positive");
					context.write(word, one);
				}
			}
			for(String str: str_stop_words ){
				if(speechWord.equalsIgnoreCase(str)){
					word.set("Stop");
					context.write(word, one);
				}
			}
			
		}
		
	}
}

// Après la phase de sorting and shuffling la methode reduce() prend comme entré (clé, valeur), le sentiment comme clé et liste des valeur 1 comme valeur
public static class sentiment_analys_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private	IntWritable	result	=	new	IntWritable();	
	public void reduce(Text	key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int	sum	=	0;								
		for	(IntWritable	val	:	values)	{										
			sum	+=	val.get();								
			}							
		result.set(sum);								
		context.write(key,	result);	
	}
}

}
