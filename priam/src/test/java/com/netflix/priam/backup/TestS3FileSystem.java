/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam.backup;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.aws.DataPart;
import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.aws.S3PartUploader;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.merics.BackupMetrics;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import mockit.Mock;
import mockit.MockUp;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestS3FileSystem {
    private static Injector injector;
    private static final Logger logger = LoggerFactory.getLogger(TestS3FileSystem.class);
    private static final String FILE_PATH =
            "target/data/Keyspace1/Standard1/backups/201108082320/Keyspace1-Standard1-ia-1-Data.db";
    private static BackupMetrics backupMetrics;
    private static String region;
    private static IConfiguration configuration;

    public TestS3FileSystem() {
        if (injector == null) injector = Guice.createInjector(new BRTestModule());

        if (backupMetrics == null) backupMetrics = injector.getInstance(BackupMetrics.class);

        if (configuration == null) configuration = injector.getInstance(IConfiguration.class);

        InstanceInfo instanceInfo = injector.getInstance(InstanceInfo.class);
        region = instanceInfo.getRegion();
    }

    @BeforeClass
    public static void setup() throws InterruptedException, IOException {
        new MockS3PartUploader();
        new MockAmazonS3Client();

        File dir1 = new File("target/data/Keyspace1/Standard1/backups/201108082320");
        if (!dir1.exists()) dir1.mkdirs();
        File file = new File(FILE_PATH);
        long fiveKB = (5L * 1024);
        byte b = 8;
        BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(file));
        for (long i = 0; i < fiveKB; i++) {
            bos1.write(b);
        }
        bos1.close();
    }

    @AfterClass
    public static void cleanup() {
        File file = new File(FILE_PATH);
        file.delete();
    }

    @Test
    public void testFileUpload() throws Exception {
        MockS3PartUploader.setup();
        IBackupFileSystem fs = injector.getInstance(NullBackupFileSystem.class);
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(new File(FILE_PATH), BackupFileType.SNAP);
        long noOfFilesUploaded = backupMetrics.getUploadRate().count();
        fs.uploadFile(
                Paths.get(backupfile.getBackupFile().getAbsolutePath()),
                Paths.get(backupfile.getRemotePath()),
                backupfile,
                0,
                false);
        // Assert.assertEquals(1, backupMetrics.getUploadRate().count() - noOfFilesUploaded);
    }

    @Test
    public void testFileUploadDeleteExists() throws Exception {
        MockS3PartUploader.setup();
        IBackupFileSystem fs = injector.getInstance(NullBackupFileSystem.class);
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(new File(FILE_PATH), BackupFileType.SST_V2);
        fs.uploadFile(
                Paths.get(backupfile.getBackupFile().getAbsolutePath()),
                Paths.get(backupfile.getRemotePath()),
                backupfile,
                0,
                false);
        Assert.assertTrue(fs.checkObjectExists(Paths.get(backupfile.getRemotePath())));
        // Lets delete the file now.
        List<Path> deleteFiles = Lists.newArrayList();
        deleteFiles.add(Paths.get(backupfile.getRemotePath()));
        fs.deleteRemoteFiles(deleteFiles);
        Assert.assertFalse(fs.checkObjectExists(Paths.get(backupfile.getRemotePath())));
    }

    @Test
    public void testFileUploadFailures() throws Exception {
        MockS3PartUploader.setup();
        MockS3PartUploader.partFailure = true;
        long noOfFailures = backupMetrics.getInvalidUploads().count();
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        String snapshotfile =
                "target/data/Keyspace1/Standard1/backups/201108082320/Keyspace1-Standard1-ia-1-Data.db";
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(new File(snapshotfile), BackupFileType.SNAP);
        try {
            fs.uploadFile(
                    Paths.get(backupfile.getBackupFile().getAbsolutePath()),
                    Paths.get(backupfile.getRemotePath()),
                    backupfile,
                    0,
                    false);
        } catch (BackupRestoreException e) {
            // ignore
        }
        Assert.assertEquals(0, MockS3PartUploader.compattempts);
        // Assert.assertEquals(1, backupMetrics.getInvalidUploads().count() - noOfFailures);
    }

    @Test
    public void testFileUploadCompleteFailure() throws Exception {
        MockS3PartUploader.setup();
        MockS3PartUploader.completionFailure = true;
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        fs.setS3Client(new MockAmazonS3Client().getMockInstance());
        String snapshotfile =
                "target/data/Keyspace1/Standard1/backups/201108082320/Keyspace1-Standard1-ia-1-Data.db";
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(new File(snapshotfile), BackupFileType.SNAP);
        try {
            fs.uploadFile(
                    Paths.get(backupfile.getBackupFile().getAbsolutePath()),
                    Paths.get(backupfile.getRemotePath()),
                    backupfile,
                    0,
                    false);
        } catch (BackupRestoreException e) {
            // ignore
        }
    }

    @Test
    public void testCleanupAdd() throws Exception {
        MockAmazonS3Client.setRuleAvailable(false);
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        fs.cleanup();
        Assert.assertEquals(1, MockAmazonS3Client.bconf.getRules().size());
        BucketLifecycleConfiguration.Rule rule = MockAmazonS3Client.bconf.getRules().get(0);
        Assert.assertEquals("casstestbackup/" + region + "/fake-app/", rule.getId());
        Assert.assertEquals(configuration.getBackupRetentionDays(), rule.getExpirationInDays());
    }

    @Test
    public void testCleanupIgnore() throws Exception {
        MockAmazonS3Client.setRuleAvailable(true);
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        fs.cleanup();
        Assert.assertEquals(1, MockAmazonS3Client.bconf.getRules().size());
        BucketLifecycleConfiguration.Rule rule = MockAmazonS3Client.bconf.getRules().get(0);
        Assert.assertEquals("casstestbackup/" + region + "/fake-app/", rule.getId());
        Assert.assertEquals(configuration.getBackupRetentionDays(), rule.getExpirationInDays());
    }

    @Test
    public void testCleanupUpdate() throws Exception {
        MockAmazonS3Client.setRuleAvailable(true);
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        String clusterPrefix = "casstestbackup/" + region + "/fake-app/";
        MockAmazonS3Client.updateRule(
                MockAmazonS3Client.getBucketLifecycleConfig(clusterPrefix, 2));
        fs.cleanup();
        Assert.assertEquals(1, MockAmazonS3Client.bconf.getRules().size());
        BucketLifecycleConfiguration.Rule rule = MockAmazonS3Client.bconf.getRules().get(0);
        Assert.assertEquals("casstestbackup/" + region + "/fake-app/", rule.getId());
        Assert.assertEquals(configuration.getBackupRetentionDays(), rule.getExpirationInDays());
    }

    @Test
    public void testDeleteObjects() throws Exception {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        List<Path> filesToDelete = new ArrayList<>();
        // Empty files
        fs.deleteRemoteFiles(filesToDelete);

        // Lets add some random files now.
        filesToDelete.add(Paths.get("a.txt"));
        fs.deleteRemoteFiles(filesToDelete);

        // Emulate error now.
        try {
            MockAmazonS3Client.emulateError = true;
            fs.deleteRemoteFiles(filesToDelete);
            Assert.assertTrue(false);
        } catch (BackupRestoreException e) {
            Assert.assertTrue(true);
        }
    }

    // Mock Nodeprobe class
    static class MockS3PartUploader extends MockUp<S3PartUploader> {
        static int compattempts = 0;
        static int partAttempts = 0;
        static boolean partFailure = false;
        static boolean completionFailure = false;
        private static List<PartETag> partETags;

        @Mock
        public void $init(AmazonS3 client, DataPart dp, List<PartETag> partETags) {
            MockS3PartUploader.partETags = partETags;
        }

        @Mock
        private Void uploadPart() throws AmazonClientException, BackupRestoreException {
            ++partAttempts;
            if (partFailure) throw new BackupRestoreException("Test exception");
            partETags.add(new PartETag(0, null));
            return null;
        }

        @Mock
        public CompleteMultipartUploadResult completeUpload() throws BackupRestoreException {
            ++compattempts;
            if (completionFailure) throw new BackupRestoreException("Test exception");

            return null;
        }

        @Mock
        public Void retriableCall() throws AmazonClientException, BackupRestoreException {
            logger.info("MOCK UPLOADING...");
            return uploadPart();
        }

        static void setup() {
            compattempts = 0;
            partAttempts = 0;
            partFailure = false;
            completionFailure = false;
        }
    }

    static class MockAmazonS3Client extends MockUp<AmazonS3Client> {
        private boolean ruleAvailable = false;
        static BucketLifecycleConfiguration bconf;
        static boolean emulateError = false;

        @Mock
        public InitiateMultipartUploadResult initiateMultipartUpload(
                InitiateMultipartUploadRequest initiateMultipartUploadRequest)
                throws AmazonClientException {
            return new InitiateMultipartUploadResult();
        }

        public PutObjectResult putObject(PutObjectRequest putObjectRequest)
                throws SdkClientException {
            PutObjectResult result = new PutObjectResult();
            result.setETag("ad");
            return result;
        }

        @Mock
        public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
            return bconf;
        }

        @Mock
        public void setBucketLifecycleConfiguration(
                String bucketName, BucketLifecycleConfiguration bucketLifecycleConfiguration) {
            bconf = bucketLifecycleConfiguration;
        }

        @Mock
        public DeleteObjectsResult deleteObjects(DeleteObjectsRequest var1)
                throws SdkClientException, AmazonServiceException {
            if (emulateError) throw new AmazonServiceException("Unable to reach AWS");
            return null;
        }

        static BucketLifecycleConfiguration.Rule getBucketLifecycleConfig(
                String prefix, int expirationDays) {
            return new BucketLifecycleConfiguration.Rule()
                    .withExpirationInDays(expirationDays)
                    .withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(prefix)))
                    .withStatus(BucketLifecycleConfiguration.ENABLED)
                    .withId(prefix);
        }

        static void setRuleAvailable(boolean ruleAvailable) {
            if (ruleAvailable) {
                bconf = new BucketLifecycleConfiguration();
                if (bconf.getRules() == null) bconf.setRules(Lists.newArrayList());

                List<BucketLifecycleConfiguration.Rule> rules = bconf.getRules();
                String clusterPath = "casstestbackup/" + region + "/fake-app/";

                List<BucketLifecycleConfiguration.Rule> potentialRules =
                        rules.stream()
                                .filter(rule -> rule.getId().equalsIgnoreCase(clusterPath))
                                .collect(Collectors.toList());
                if (potentialRules == null || potentialRules.isEmpty())
                    rules.add(
                            getBucketLifecycleConfig(
                                    clusterPath, configuration.getBackupRetentionDays()));
            }
        }

        static void updateRule(BucketLifecycleConfiguration.Rule updatedRule) {
            List<BucketLifecycleConfiguration.Rule> rules = bconf.getRules();
            Optional<BucketLifecycleConfiguration.Rule> updateRule =
                    rules.stream()
                            .filter(rule -> rule.getId().equalsIgnoreCase(updatedRule.getId()))
                            .findFirst();
            if (updateRule.isPresent()) {
                rules.remove(updateRule.get());
                rules.add(updatedRule);
            } else {
                rules.add(updatedRule);
            }
            bconf.setRules(rules);
        }
    }
}
