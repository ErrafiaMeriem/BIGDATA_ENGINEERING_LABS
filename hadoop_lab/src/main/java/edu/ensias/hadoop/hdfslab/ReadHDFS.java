package edu.ensias.hadoop.hdfslab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        // Vérifier que le nom du fichier est passé en paramètre
        if (args.length < 1) {
            System.out.println("Usage: hadoop jar ReadHDFS.jar <chemin_fichier>");
            System.exit(1);
        }

        String cheminFichier = args[0]; // ex: ./purchases.txt ou /user/root/purchases.txt

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(cheminFichier);

        if (!fs.exists(filePath)) {
            System.out.println("File does not exist: " + filePath);
            System.exit(1);
        }

        // Lecture du fichier
        FSDataInputStream inStream = fs.open(filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
        String line;

        System.out.println("Contenu du fichier " + filePath + " :");
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        br.close();
        inStream.close();
        fs.close();
    }
}
