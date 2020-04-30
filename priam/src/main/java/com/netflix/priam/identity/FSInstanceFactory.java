package com.netflix.priam.identity;

import com.google.gson.stream.JsonReader;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.priam.backup.AbstractFileSystem;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.GsonJsonSerializer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FSInstanceFactory implements IPriamInstanceFactory<PriamInstance> {
    private static final Logger logger = LoggerFactory.getLogger(FSInstanceFactory.class);

    private final IConfiguration config;
    private final IBackupFileSystem fileSystem;
    private final InstanceInfo instanceInfo;

    @Inject
    public FSInstanceFactory(
            IConfiguration config, InstanceInfo instanceInfo, @Named("backup") IBackupFileSystem fileSystem) {
        this.config = config;
        this.instanceInfo = instanceInfo;
        this.fileSystem = fileSystem;
    }

    @Override
    public List<PriamInstance> getAllIds(String appName) {
        String remotePathPrefix = remotePath(appName, null, null);
        Iterator<String> fileIterator =
                this.fileSystem.listFileSystem(remotePathPrefix, null, null);
        List<PriamInstance> insts = new ArrayList<>();
        fileIterator.forEachRemaining(
                (path) -> {
                    Path remoteFilePath = Paths.get(path);
                    String dc = remoteFilePath.getName(2).toString();
                    Integer id = Integer.valueOf(remoteFilePath.getName(3).toString());
                    insts.add(getInstance(appName, dc, id));
                });
        return insts;
    }

    @Override
    public PriamInstance getInstance(String appName, String dc, int id) {
        Path localPath = Paths.get(remotePath(appName, dc, id));
        Path remotePath = Paths.get(localPath(appName, dc, id));
        try {
            this.fileSystem.downloadFile(localPath, remotePath, 3);
        } catch (Exception e) {
            logger.error(
                    "Error while trying to upload {} to {}. Error: {} ",
                    localPath,
                    remotePath,
                    e.getLocalizedMessage());
        }
        try {
            JsonReader jsonReader = new JsonReader(new FileReader(localPath.toFile()));
            jsonReader.beginObject();
            JSONObject jsn = GsonJsonSerializer.getGson().fromJson(jsonReader, PriamInstance.class);
            return fromJson(jsn);
        } catch (Exception e) {
            logger.error(
                    "Error while trying to get instance info for app {} dc {} id {}. Error: {}",
                    appName,
                    dc,
                    id,
                    e.getLocalizedMessage());
        }
        return null;
    }

    @Override
    public PriamInstance create(
            String app,
            int id,
            String instanceID,
            String hostname,
            String ip,
            String rac,
            Map<String, Object> volumes,
            String token) {
        PriamInstance inst =
                makePriamInstance(app, id, instanceID, hostname, ip, rac, volumes, token);
        uploadPriamInstanceToFS(inst);
        return inst;
    }

    @Override
    public void delete(PriamInstance inst) {
        List<Path> paths = new ArrayList<>();
        paths.add(inst.pathInRemote());
        try {
            this.fileSystem.deleteRemoteFiles(paths);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void update(PriamInstance inst) {
        uploadPriamInstanceToFS(inst);
    }

    @Override
    public void sort(List<PriamInstance> return_) {
        Comparator<? super PriamInstance> comparator =
                (Comparator<PriamInstance>)
                        (o1, o2) -> {
                            Integer c1 = o1.getId();
                            Integer c2 = o2.getId();
                            return c1.compareTo(c2);
                        };
        return_.sort(comparator);
    }

    @Override
    public void attachVolumes(PriamInstance instance, String mountPath, String device) {
        //
    }

    private void uploadPriamInstanceToFS(PriamInstance inst) {
        String jsn = inst.toJson().toJSONString();
        Path tempFilePath = Paths.get(this.config.getRemoteFSInstancePrefix() + inst.pathInLocal());
        File tmpFile = tempFilePath.toFile();
        if (!tmpFile.exists()) tmpFile.getParentFile().mkdirs();

        try (final ObjectOutputStream out =
                new ObjectOutputStream(new FileOutputStream(tmpFile.getName()))) {
            out.writeObject(jsn);
            out.flush();
            logger.info("InstanceInfo of size {} is saved to {}", jsn.length(), tmpFile.getName());
        } catch (IOException e) {
            logger.error(
                    "Error while trying to persist instance info to {}. Error: {}",
                    tmpFile.getName(),
                    e.getLocalizedMessage());
        }
        try {
            this.fileSystem.asyncUploadFile(tempFilePath, inst.pathInRemote(), null, 3, true);
        } catch (Exception e) {
            logger.error(
                    "Error while trying to upload {} to {}. Error: {} ",
                    tempFilePath,
                    inst.pathInRemote(),
                    e.getLocalizedMessage());
        }
    }

    private PriamInstance makePriamInstance(
            String app,
            int id,
            String instanceID,
            String hostname,
            String ip,
            String rac,
            Map<String, Object> volumes,
            String token) {
        Map<String, Object> v = (volumes == null) ? new HashMap<>() : volumes;
        PriamInstance ins = new PriamInstance();
        ins.setApp(app);
        ins.setRac(rac);
        ins.setHost(hostname);
        ins.setHostIP(ip);
        ins.setId(id);
        ins.setInstanceId(instanceID);
        ins.setDC(instanceInfo.getRegion());
        ins.setToken(token);
        ins.setVolumes(v);
        return ins;
    }

    private String localPath(String app, String dc, Integer id) {
        return appendParts(this.config.getLocalFSInstancePrefix(), app, dc, id);
    }

    private String remotePath(String app, String dc, Integer id) {
        return appendParts(this.config.getRemoteFSInstancePrefix(), app, dc, id);
    }

    private String appendParts(String prefix, String app, String dc, Integer id) {
        StringBuilder path = new StringBuilder(prefix);
        if (app != null && !app.isEmpty()) {
            path.append('/');
            path.append(app);
        }
        if (dc != null && !dc.isEmpty()) {
            path.append('/');
            path.append(dc);
        }
        if (id != null) {
            path.append('/');
            path.append(id);
        }
        return path.toString();
    }

    public PriamInstance fromJson(JSONObject jsn) {
        return makePriamInstance(
                (String) jsn.get("app"),
                (Integer) jsn.get("id"),
                (String) jsn.get("instanceId"),
                (String) jsn.get("hostname"),
                (String) jsn.get("ip"),
                (String) jsn.get("rac"),
                null,
                (String) jsn.get("token"));
    }
}
