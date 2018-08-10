package demo.fansy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author: fansy
 * @Time: 2018/8/9 17:22
 * @Email: fansy1990@foxmail.com
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Logger logger = LoggerFactory.getLogger(WordCountMapper.class);
    private int num = 0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        num = context.getConfiguration().getInt("arg.num",-1);
        logger.info("num:{}",num);
    }
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
