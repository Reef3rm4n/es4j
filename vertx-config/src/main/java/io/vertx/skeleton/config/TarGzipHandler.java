/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.skeleton.config;

import io.vertx.skeleton.models.exceptions.VertxServiceException;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

public class TarGzipHandler {

  private TarGzipHandler() {

  }

  public static String compress(List<String> filePaths, String outputFilePath) {
    try {
      try (OutputStream fOut = Files.newOutputStream(Path.of(outputFilePath));
           BufferedOutputStream buffOut = new BufferedOutputStream(fOut);
           GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(buffOut);
           TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {

        for (String filePath : filePaths) {
          final var path = Path.of(filePath);
          if (!Files.isRegularFile(path)) {
            throw new IOException("Support only file!");
          }
          TarArchiveEntry tarEntry = new TarArchiveEntry(path.toFile(), path.getFileName().toString());
          tOut.putArchiveEntry(tarEntry);
          Files.copy(path, tOut);
          tOut.closeArchiveEntry();
        }
        tOut.finish();
      }
    } catch (Exception e) {
      throw new VertxServiceException("unable to compress files", e.getMessage(), 500);
    }
    return outputFilePath;
  }

  public static String decompress(String source, String targetDir) {
    try {
      if (Files.notExists(Path.of(source))) {
        throw new IOException("File doesn't exists!");
      }

      try (InputStream fi = Files.newInputStream(Path.of(source));
           BufferedInputStream bi = new BufferedInputStream(fi);
           GzipCompressorInputStream gzi = new GzipCompressorInputStream(bi);
           TarArchiveInputStream ti = new TarArchiveInputStream(gzi)) {

        ArchiveEntry entry;
        while ((entry = ti.getNextEntry()) != null) {

          // create a new path, zip slip validate
          Path newPath = zipSlipProtect(entry, Path.of(targetDir));

          if (entry.isDirectory()) {
            Files.createDirectories(newPath);
          } else {

            // check parent folder again
            Path parent = newPath.getParent();
            if (parent != null && Files.notExists(parent)) {
              Files.createDirectories(parent);
            }
            // copy TarArchiveInputStream to Path newPath
            Files.copy(ti, newPath, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      }
    } catch (Exception e) {
      throw new VertxServiceException("unable to decompress files", e.getMessage(), 500);
    }
    return targetDir;
  }

  private static Path zipSlipProtect(ArchiveEntry entry, Path targetDir) throws IOException {
    Path targetDirResolved = targetDir.resolve(entry.getName());
    // make sure normalized file still has targetDir as its prefix,
    // else throws exception
    Path normalizePath = targetDirResolved.normalize();
    if (!normalizePath.startsWith(targetDir)) {
      throw new IOException("Bad entry: " + entry.getName());
    }

    return normalizePath;
  }
}
