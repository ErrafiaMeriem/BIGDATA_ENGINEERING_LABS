package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {

        // Vérifier si le fichier est passé en paramètre (optionnel)
        String chemin = "/user/root/input";
        String nomFichier = "purchases.txt";
        String nouveauNom = "achats.txt";

        Configuration conf = new Configuration();
        FileSystem fs;

        try {
            fs = FileSystem.get(conf);
            Path filepath = new Path(chemin, nomFichier);

            // Vérifier l'existence du fichier
            if (!fs.exists(filepath)) {
                System.out.println("File does not exist");
                System.exit(1);
            }

            // Récupérer les informations du fichier
            FileStatus status = fs.getFileStatus(filepath);

            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File owner: " + status.getOwner());
            System.out.println("File permission: " + status.getPermission());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize());

            // Infos sur les blocs
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            // Renommer le fichier
            Path newPath = new Path(chemin, nouveauNom);
            if (fs.rename(filepath, newPath)) {
                System.out.println("File renamed successfully to: " + nouveauNom);
            } else {
                System.out.println("Failed to rename file");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
