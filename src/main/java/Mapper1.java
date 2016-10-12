import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, LongWritable>{

    // objet LongWritable pour la valeur
    private LongWritable one = new LongWritable(1);
    // objet Text pour la cl�
    private Text word = new Text();

    // map(key, eache line, mapper context)
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
        // on s�parer chq cat�gorie d�limit�e par un ; -> first name;genders;origins;value
        String[] split = line.toString().split(";");

        // on prend la 3e case qui est l'origine et on s�pare chq origine
        String[] origins = split[2].split(",");

        // pour chaque entr�e de origins, on cr�e une paire (origins, value)
        for(String origin: origins){
            try {
                word.set(origin); // on set l'�l�ment origin dans un objet Text pour pouvoir ensuite l'�crire dans le context
                context.write(word, one); // �criture de la paire (origine, one) dans le context
            } catch (InterruptedException e) {
            }
        }
    }
}
