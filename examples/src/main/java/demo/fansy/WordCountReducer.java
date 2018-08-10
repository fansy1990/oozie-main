package demo.fansy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author: fansy
 * @Time: 2018/8/9 17:27
 * @Email: fansy1990@foxmail.com
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private Logger logger = LoggerFactory.getLogger(WordCountReducer.class);
    private int num = 0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        num = context.getConfiguration().getInt("arg.num",-1);
        logger.info("reducer num:{}",num);
    }
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        if(sum> num) {
            result.set(sum);
            context.write(key, result);
        }
    }
}
