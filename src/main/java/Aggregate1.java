import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Aggregate1 extends Configured implements Tool {

    // on arrive ici après le lancement de ToolRunner.run(new Exo...)
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(getClass());
        job.setJobName(getClass().getSimpleName());

        // input path correspond à args[0] = le fichier à étudier
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // output path correspond à args[1] = le fichier de résultats
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // on set le mapper qui va mapper l'input
        job.setMapperClass(Mapper1.class);
        // on set le reducer qui va faire office de combiner pour chq map => va compacter chq map en fait
        job.setCombinerClass(Reducer1.class);
        // on set encore le reducer mais cette fois-ci pour le reduce job qui va reduce toutes les maps ensemble
        job.setReducerClass(Reducer1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // lance un tool avec une configuration : Exo1AggregateJob est un objet Tool, et on lui donne les paramètres renseignées dans le terminal (args)
        int rc = ToolRunner.run(new Aggregate1(), args);
        System.exit(rc);
    }
}