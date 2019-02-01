package com.example.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapSideJoin extends Configured implements Tool {
    Logger logger = Logger.getLogger(this.getClass());

    public static class MSJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Map<String, String> secondTable = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Path[] localPaths = context.getLocalCacheFiles();
            if (localPaths.length > 0)
                //Read File and populate secondtable map
                readFile(localPaths[0].toString());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String firstTblLine = value.toString();
            //Ignore Header
            if (!firstTblLine.contains("Pclass")) {
                String firstTblCols[] = firstTblLine.trim().split(",");
                String fTblPClass = firstTblCols[2];
                String sTblClassDesc = secondTable.getOrDefault(fTblPClass, "NOT FOUND");
                context.write(new Text(firstTblLine + "," + sTblClassDesc), NullWritable.get());
            } else {
                context.write(new Text(firstTblLine + "," + "classDescription"), NullWritable.get());
            }
        }

        private void readFile(String filePath) {
            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    //Ignore header
                    if (!line.contains("Pclass,classDescription")) {
                        String lineArr[] = line.trim().split(",");
                        secondTable.put(lineArr[0], lineArr[1]);
                    }
                }
            } catch (IOException ex) {
                System.err.println("Exception while reading stop words file: " + ex.getMessage());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MapSideJoin(), args);
        System.exit(res);
    }


    public int run(String[] args) throws Exception {
        logger.info("Entering run method");
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "MapSide Join Example");
            job.setJarByClass(MapSideJoin.class);
            job.setMapperClass(MSJMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            List<String> other_args = new ArrayList<>();
            for (int i = 0; i < args.length; ++i) {
                try {
                    if ("-r".equals(args[i])) {
                        job.setNumReduceTasks(Integer.parseInt(args[++i]));
                    } else {
                        other_args.add(args[i]);
                    }
                } catch (NumberFormatException except) {
                    System.out.println("ERROR: Integer expected instead of " + args[i]);
                    return printUsage();
                } catch (ArrayIndexOutOfBoundsException except) {
                    System.out.println("ERROR: Required parameter missing from " +
                            args[i - 1]);
                    return printUsage();
                }
            }
            // Make sure there are exactly 2 parameters left.
//            if (other_args.size() != 2) {
//                System.out.println("ERROR: Wrong number of parameters: " +
//                        other_args.size() + " instead of 2.");
//                return printUsage();
//            }
            job.setNumReduceTasks(0);
            FileInputFormat.setInputPaths(job, other_args.get(0));
            //Add Smaller Dataset to Distributed Cache
            job.addCacheFile(new URI(other_args.get(1)));

            FileOutputFormat.setOutputPath(job, new Path(other_args.get(2)));


            return (job.waitForCompletion(true) ? 0 : 1);
        } finally {
            logger.info("Exiting run method");
        }
    }

    static int printUsage() {
        System.out.println("Error: Something wrong !");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;

    }
}
