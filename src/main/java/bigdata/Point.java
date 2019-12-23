package bigdata;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
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

public class Point{
    List<Double> coords = new ArrayList<Double>(); // List of the coordinates of the point
    
    // Default constructor
    public Point(){} 

    // "d1,d2,....,dn" -> point of coordinates d1,d2,....,dn
    public Point(String s){
	for(String coord : s.split(",")){
	    coords.add(Double.parseDouble(coord));
	}
    }

    // "d1,d2,....,dn" -> point of coordinates d1,d2,....,dn
    public Point(StringBuffer s){
	for(String coord : s.toString().split(",")){
	    coords.add(Double.parseDouble(coord));
	}
    }

    // compute ||this - p|| for the norm 2
    public double dist2(Point p){
	double res = 0;
	int i = 0;
	for(double coord : coords){
	    res += (coord - p.coords.get(i)) * (coord - p.coords.get(i));
	    i++;
	}
	return Math.sqrt(res);
    }


    // Retrun true if this and p are consider to be the same
    public boolean isNear(Point p){
	return (this.dist2(p) <= 1e-16);
    }


    // Return the nearest point to this in means
    public Point meanNearest(List<Point> means){
	double d = this.dist2(means.get(0));
	Point nearest = means.get(0);
	for(Point mean: means){
	    double d_tmp = this.dist2(mean);
	    if(d_tmp < d){
		d = d_tmp;
		nearest = mean;
	    }
	}
	return nearest;
    }
    
    // Return the index of the nearest point to this in means
    public int indexOfNearestMean(List<Point> means){
	Point nearestMean = this.meanNearest(means);
	return means.indexOf(nearestMean);
    }
    
    // If this is not initialise then do this[i] = p[i]
    // Else do this[i] += p[i]
    public void add(Point p){
	if(coords.size() == 0){
	    for(int i = 0; i < p.coords.size(); i++){
		coords.add(p.coords.get(i));
	  }
	    return;
	}
	
	List<Double> tmp = new ArrayList<Double>();
	for(int i = 0; i < coords.size(); i++){
	  tmp.add(coords.get(i) + p.coords.get(i));
	}
	coords = tmp;
    }

    // Divide each coordinates of this by d
    public void div(double d){
	List<Double> tmp = new ArrayList<Double>();
	for(int i = 0; i < coords.size(); i++){
	    tmp.add(coords.get(i) / d);
	}
	coords = tmp;
    }
    
    // point of coordinates d1,d2,....,dn -> "d1,d2,....,dn"
    public String toString(){
	StringBuffer res = new StringBuffer(coords.get(0).toString());
	for(int i = 1; i < coords.size(); i++){
	  res.append("," + coords.get(i));
	}
	return new String(res);
    }


}
