package com.hadoop.train;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 统计相同字母组成的所有单词
 * 1、编写Map()函数
 * 2、编写Reduce()函数
 * 3、编写run()函数
 * 4、编写main()方法
 */
public class Anagram{

    public static class AnagramMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text sortedText = new Text();
        private Text orginalText = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            //将单词转换为字符串，然后存入字符数组
            String word = value.toString();
            char[] wordChars = word.toCharArray();
            //排序
            Arrays.sort(wordChars);
            //将排好序的字符数组转换为字符串类型，此时为一个Word按其字母排序后组成的另一个Word，然后存入sortedText中
            String sortedWord = new String(wordChars);
            sortedText.set(sortedWord);
            //将原始字母存入orginalText
            orginalText.set(word);
            
            context.write(sortedText, orginalText);
        }
    }
    
    public static class AnagramReducer extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //将相同字母组成的单词用"~"符号链接起来
            String output = "";
            for(Text anagram:values) {
                if (!output.equals("")) {
                    output = output + "~";
                }
                output += anagram.toString();
            }
            
            StringTokenizer outputTokenizer = new StringTokenizer(output, "~");
            //过滤掉只有一个字母的单词
            if (outputTokenizer.countTokens() >= 2) {
                output = output.replaceAll("~", ",");
                
                context.write(key, new Text(output));
            }
        }
    }
    
    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
 Job job = Job.getInstance(conf, "word count");
 job.setJarByClass(Anagram.class);
 job.setMapperClass(AnagramMapper.class);
 job.setReducerClass(AnagramReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1); 

        
    }

}

