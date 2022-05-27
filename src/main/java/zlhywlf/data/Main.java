package zlhywlf.data;

import org.apache.commons.lang.ArrayUtils;
import zlhywlf.data.base.IJob;
import zlhywlf.data.hdfs.Mkdirs;
import zlhywlf.data.jobs.WordCount;

/**
 * @author zlhywlf
 */
public class Main {
    private static final String[] JOBS = new String[]{WordCount.class.getSimpleName()};
    private static final String[] DFS = new String[]{Mkdirs.class.getSimpleName()};

    public static void printHelp() {
        String help = "hadoop jar big-data-1.0.0.jar [jobName] [...args]\n" +
                Mkdirs.class.getSimpleName() + " [path] : Create a directory\n" +
                WordCount.class.getSimpleName() + " [input] [output] [...args] : Word Count \n" +
                "hadoop jar big-data.jar [jobName] [...args]\n" +
                "hadoop jar big-data.jar [jobName] [...args]\n";
        System.out.println(help);
    }

    public static void main(String[] args) {
        if (args.length > 0) {
            run(args);
        } else {
            printHelp();
        }
        System.out.println("\nresources:https://github.com/zlhywlf/big-data");
    }

    public static void run(String[] args) {
        String t = args[0];
        boolean hasJob = ArrayUtils.contains(JOBS, t);
        boolean hasDfs = ArrayUtils.contains(DFS, t);
        try {
            if (hasJob) {
                run("zlhywlf.data.jobs.", args);
            } else if (hasDfs) {
                run("zlhywlf.data.hdfs.", args);
            } else {
                printHelp();
            }
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
    }

    public static void run(String pre, String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Object o = Class.forName(pre + args[0]).newInstance();
        if (o instanceof IJob) {
            ((IJob) o).driver(args);
        }
    }
}