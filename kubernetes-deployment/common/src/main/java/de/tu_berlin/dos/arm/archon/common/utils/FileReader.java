package de.tu_berlin.dos.arm.archon.common.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

public enum FileReader { GET;

    public static final Logger LOG = LogManager.getLogger(FileReader.class);

    private static final Map<String, Object> fileMap = new HashMap<>();

    public String path(String fileName) throws Exception {

        URL resource = FileReader.GET.getClass().getClassLoader().getResource(fileName);
        return resource.getFile();
    }

    public <T> T read(String fileName, Class<T> clazz) throws Exception {

        if (!fileMap.containsKey(fileName)) {
            try (InputStream input = FileReader.GET.getClass().getClassLoader().getResourceAsStream(fileName)) {
                if (clazz == Properties.class) {

                    Object o = OrderedProperties.class.getDeclaredConstructor().newInstance();
                    Method method = clazz.getMethod("load", InputStream.class);
                    method.invoke(o, input);
                    fileMap.put(fileName, o);
                }
                if (clazz == OrderedProperties.class) {

                    Object o = OrderedProperties.class.getDeclaredConstructor().newInstance();
                    Method method = clazz.getMethod("load", InputStream.class);
                    method.invoke(o, input);
                    fileMap.put(fileName, o);
                }
                else if (clazz == File.class) {

                    final File tempFile = File.createTempFile("stream2file", ".tmp");
                    tempFile.deleteOnExit();
                    try (FileOutputStream output = new FileOutputStream(tempFile)) {
                        assert input != null;
                        IOUtils.copy(input, output);
                    }
                    fileMap.put(fileName, tempFile);
                }
                else {
                    throw new IllegalStateException("Unrecognized file type: " + clazz);
                }
            }
            catch (IOException ex) {
                LOG.error(ex);
                throw ex;
            }
        }
        return clazz.cast(fileMap.get(fileName));
    }

    public String toString(File file) throws Exception {

        return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    }
}
