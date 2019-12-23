package bigdata;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Kmeans {


    /////////////////////////////// MAPPERS and REDUCERS /////////////////////////////////////

    // Mapper that compute the centroid afiliated with each point
    public static class MapperMerge extends Mapper<LongWritable, Text, IntWritable, Text>{
	List<Point> means = new ArrayList<Point>();
	List<Integer> cols = new ArrayList<Integer>();

	// Computing centroids and columns to use
	protected void setup(Context context) throws IOException {
	    Configuration conf = context.getConfiguration();
	    FileSystem hdfs = FileSystem.get(conf);
	    Path centroids_file = new Path(conf.get("centroids_file"));
	    FSDataInputStream inputStream = hdfs.open(centroids_file);

	    // Computing columns
	    for(String coord : conf.get("cols").split(",")){
		cols.add(Integer.parseInt(coord));
	    }

	    // Computing centroids
	    String line;
	    while((line = inputStream.readLine()) != null) {
		means.add(new Point(line));
	    }

	    inputStream.close();
	}


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    // The first line is not to be consider
	    if(key.get() == 0) return;

	    // Creating a sub-string containing only columns of intrest
	    String tokens[] = value.toString().split(",");
	    //if (tokens.length < 7) return;

	    StringBuffer coords = new StringBuffer();
	    int i = 0;
	    for(int col : cols){
		      if(tokens[col].length() == 0) return;
		        if(i == 0){
		            coords.append(tokens[col]);
		         }else{
		             coords.append("," + tokens[col]);
             }
	           i = 1;
	    }


	    Point p = new Point(coords);
	    context.write(new IntWritable(p.indexOfNearestMean(means)), new Text(p.toString()));

	}
    }

    // Reducer that write centroids file
    public static class ReducerMerge extends Reducer<IntWritable, Text, NullWritable, Text> {
	List<Point> means = new ArrayList<Point>(); // List of centroids, for the log

	// Write a log of the different values of centroids
	protected void cleanup(Context context) throws IOException {
	    Configuration conf = context.getConfiguration();
	    FileSystem hdfs = FileSystem.get(conf);
	    Path log = new Path(conf.get("log_file"));
	    FSDataOutputStream outputStream;
	    if (!hdfs.exists(log)){
		outputStream = hdfs.create(log);
	    }else{
		outputStream = hdfs.append(log);
	    }


	    for(Point p : means) {
		outputStream.writeBytes(p.toString() + "\n");
	    }

	    outputStream.writeBytes("\n");
	    outputStream.close();
	}

	// Writing the diffrent centroids
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	    Point p_sum = new Point();
	    int n = 0;
	    for(Text val: values){
		p_sum.add(new Point(val.toString()));
		n++;
	    }

	    p_sum.div(n);
	means.add(p_sum); // Registering the centroid, for the log
	context.write(NullWritable.get(), new Text(p_sum.toString() + "\n"));
	}
    }


    // Mapper that add the corresponding centroid to each line
    public static class MapperWriteAnswer extends Mapper<LongWritable, Text, NullWritable, Text>{
	List<Point> means = new ArrayList<Point>();
	List<Integer> cols = new ArrayList<Integer>();

	// Computing centroids and columns to use
	protected void setup(Context context) throws IOException {
	    Configuration conf = context.getConfiguration();
	    FileSystem hdfs = FileSystem.get(conf);
	    Path centroids_file = new Path(conf.get("centroids_file"));
	    FSDataInputStream inputStream = hdfs.open(centroids_file);

	    // Computing columns
	    for(String coord : conf.get("cols").split(",")){
		cols.add(Integer.parseInt(coord));
	    }

	    // Computing centroids
	    String line;
	    while((line = inputStream.readLine()) != null) {
		means.add(new Point(line));
	    }

	    inputStream.close();
	}


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    // Adding the description of the last column
	    if(key.get() == 0) {
		context.write(NullWritable.get(), new Text(value + ",#cluster"));
		return;
	    }

	    // Creating a substring of value containing only concerned columns
	    String tokens[] = value.toString().split(",");
	    //if (tokens.length < 7) return;

	    StringBuffer coords = new StringBuffer();
	    int i = 0;
	    for(int col : cols){
		if(tokens[col].length() == 0) return;
		if(i == 0){
		    coords.append(tokens[col]);
		}else{
		    coords.append("," + tokens[col]);
		}
		i = 1;
	    }

	    // Writting the updated line
	    Point p = new Point(coords);
	    context.write(NullWritable.get(), new Text(value + "," + p.indexOfNearestMean(means)));

	}
    }

    // Reducer that write is input (no computing)
    public static class ReducerWriteAnswer extends Reducer<NullWritable, Text, NullWritable, Text> {
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    for(Text val: values){
		context.write(NullWritable.get(), val);
	    }
	}
    }



    ////////////////////////////////////////////// AUXILARY FUNCTIONS ////////////////////////////////////////////////////

    // Create a file named centroids_file with k points of nbCols dimensions
    private static void initCentroidsFile(int nbCols, int k, Path centroids_file, FileSystem hdfs) throws Exception {
	FSDataOutputStream outputStream = hdfs.create(centroids_file);
	Random r = new Random();
	for(int i = 0; i < k; i++){
	    StringBuffer coords = new StringBuffer("" + r.nextDouble());
	    for(int j = 1; j < nbCols; j++){
		coords.append("," + r.nextDouble());
	    }
	    outputStream.writeBytes(coords.toString() + "\n");
	}
	outputStream.close();
    }

    // Add the string "args[start],args[start + 1],....,args[n]" to the configuration conf at key "cols"
    private static void addColumsToConf(String[] args, int start, Configuration conf){
	int col_tmp = -1;
	StringBuffer cols = new StringBuffer();
	for(int i = start; i < args.length; i++){
	    col_tmp = Integer.parseInt(args[i]); // check the validity of argument
	    if(i == start){
		cols.append(args[i]);
	    }else{
		cols.append("," + args[i]);
	    }
	}
	conf.set(new String("cols"), new String(cols));
    }



    // Add centroids from cfile to res, if res does not all centroids
    private static void addMissingCentroids(Path cfile, Path res, FileSystem hdfs) throws Exception {

	if(!hdfs.exists(cfile) || !hdfs.exists(res)) return;

	FSDataInputStream inputStream = hdfs.open(cfile);
	FSDataInputStream inputStream2 = hdfs.open(res);

	Path tmp = new Path("tmp.csv");
	if(hdfs.exists(tmp)){
	    hdfs.delete(tmp, true);
	}
	FSDataOutputStream outputStream = hdfs.create(tmp);

	String line;
	String line2;

	while((line2 = inputStream2.readLine()) != null) {
	    if(line2.length() > 0){
		outputStream.writeBytes(line2 + "\n");
		line = inputStream.readLine();
	    }
	}

	while((line = inputStream.readLine()) != null) {
	    outputStream.writeBytes(line + "\n");
	}

	outputStream.close();
	inputStream.close();
	inputStream2.close();

	hdfs.delete(res, true);
	hdfs.rename(tmp, res);

    }

    // Return true if all centroids in file and file2 are the same
    // Return false otherwise
    private static boolean hasConverge(Path file, Path file2, FileSystem hdfs) throws Exception {

	if(!hdfs.exists(file) || !hdfs.exists(file2)) return false;

	FSDataInputStream inputStream = hdfs.open(file);
	FSDataInputStream inputStream2 = hdfs.open(file2);

	String line;
	String line2;
	int i = 0;
	while((line = inputStream.readLine()) != null && (line2 = inputStream2.readLine()) != null) {
	    Point p = new Point(line);
	    Point p2 = new Point(line2);
	    if(!p.isNear(p2)){
		inputStream.close();
		inputStream2.close();
		return false;
	    }
	    i++;
	}
	inputStream.close();
	inputStream2.close();
	return (i > 0);
    }

    private static void moveMeans(Path input, Path output, Configuration conf, FileSystem hdfs, int step) throws Exception {
	Path tmp = new Path("tmp_Kmeans");

	// Cleaning
	if(hdfs.exists(tmp)) hdfs.delete(tmp, true);

	Job job = Job.getInstance(conf, "Kmeans " + step);
	job.setNumReduceTasks(1);
	job.setJarByClass(Kmeans.class);
	job.setMapperClass(MapperMerge.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);

	job.setReducerClass(ReducerMerge.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(Text.class);

	job.setOutputFormatClass(TextOutputFormat.class);
	job.setInputFormatClass(TextInputFormat.class);
	FileInputFormat.addInputPath(job, input);
	FileOutputFormat.setOutputPath(job, tmp);
	job.waitForCompletion(true);


	// Methodes to redirect the output are protected, so this is an alternativ
	if(hdfs.exists(output)) hdfs.delete(output, true);
	hdfs.rename(new Path("tmp_Kmeans/part-r-00000"), output);
	hdfs.delete(tmp, true);

    }

    private static void writeAnswer(Path input, Path output, Configuration conf) throws Exception {
	Job job = Job.getInstance(conf, "Kmeans Write Answer");
	job.setNumReduceTasks(1);
	job.setJarByClass(Kmeans.class);
	job.setMapperClass(MapperWriteAnswer.class);

	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(Text.class);

	job.setReducerClass(ReducerWriteAnswer.class);

	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);

	job.setOutputFormatClass(TextOutputFormat.class);
	job.setInputFormatClass(TextInputFormat.class);
	FileInputFormat.addInputPath(job, input);
	FileOutputFormat.setOutputPath(job, output);
	job.waitForCompletion(true);
    }

  public static void main(String[] args) throws Exception {

      // Setting up variables
      Path input = new Path(args[0]);
      Path output = new Path(args[1]);
      int k = Integer.parseInt(args[2]);

      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(new URI("hdfs://lsd:9000"), conf);
      Path log = new Path("log.csv");
      Path centroids_file = new Path("centroids.csv");
      Path centroids_tmp = new Path("centroids_tmp.csv");
      String centroids_conf_name = new String("centroids_file");

      // Cleaning the repository
      if(hdfs.exists(centroids_tmp)) {
	  hdfs.delete(centroids_tmp, true);
      }

      if (hdfs.exists(centroids_file)){
	  hdfs.delete(centroids_file, true);
      }

      if (hdfs.exists(log)){
	  hdfs.delete(log, true);
      }

      if (hdfs.exists(output)){
	  hdfs.delete(output, true);
      }

      // Setting the configuration
      conf.set(centroids_conf_name, centroids_file.toString());
      conf.set(new String("log_file"), log.toString());
      conf.set(new String("k"), new String(args[2]));
      addColumsToConf(args, 3, conf);

      // Creating a centroid file with random number
      initCentroidsFile(args.length - 3, k, centroids_file, hdfs);



      int step = 0; // Iteration number
      Path tmp;

      while(!hasConverge(centroids_file, centroids_tmp, hdfs)){

	  // Appling kmeans algorithm
	  conf.set(centroids_conf_name, centroids_file.toString());
	  moveMeans(input, centroids_tmp, conf, hdfs, step);
	  addMissingCentroids(centroids_file, centroids_tmp, hdfs); // In some cases centroids have no closer points in the data

	  // Changing centroids file
	  tmp = centroids_file;
	  centroids_file = centroids_tmp;
	  centroids_tmp = tmp;

	  step++;
      }

      // Genarating the output
      conf.set(centroids_conf_name, centroids_file.toString());
      writeAnswer(input, output, conf);

      if(hdfs.exists(centroids_tmp)) {
	  hdfs.delete(centroids_tmp, true);
      }

      if (hdfs.exists(centroids_file)){
	  hdfs.delete(centroids_file, true);
      }
  }
}
