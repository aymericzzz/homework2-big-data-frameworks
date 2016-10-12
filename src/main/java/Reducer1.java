import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer1 extends Reducer<Text, LongWritable, Text, LongWritable> {

    // valeur après reduce
    private LongWritable result = new LongWritable();

    // reduce(key, values, context)
    public void reduce(Text key, Iterable<LongWritable> values, Context context)throws IOException, InterruptedException{
        long temp = 0;

        // on lit chaque valeur du tableau values contenant les paires ayant la même key
        // puis, on somme chaque val
        for(LongWritable val: values){
            temp += val.get();
        }

        // on set la somme des valeurs dans un objet LongWritable
        result.set(temp);
        // on écrit une nouvelle paire contenant la clé ainsi que la somme des valeurs ayant cette même clé
        context.write(key,result);
    }
}