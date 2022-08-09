package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_WHITE_LIST;

public class TestWhiteListTrash {
    private static final Logger LOG =
            LoggerFactory.getLogger(TestWhiteListTrash.class);

    private static final File base =
            GenericTestUtils.getTestDir("work-dir/localfs");

    private static final String TEST_ROOT_DIR = base.getAbsolutePath();
    private Configuration conf;
    private LocalFileSystem fileSys;

    @Before
    public void setup() throws IOException {
        conf = new Configuration(false);
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        fileSys = FileSystem.getLocal(conf);
        fileSys.delete(new Path(TEST_ROOT_DIR), true);
        conf.set(FS_TRASH_WHITE_LIST, "/Users/yuanbo/person/code/tdw/levineliu/hadoop/hadoop-common-project/hadoop-common/src/test/resources/white_list_path");
    }

    @Test
    public void testTrashPath() throws IOException {
        Path basePath = new Path(base.getAbsolutePath(), ".Trash");
        Path currentPath = new Path(basePath, "Current");
        Path testPath1 = new Path(currentPath, "a/b/c");
        fileSys.mkdirs(testPath1);
        Path testPath2 = new Path(currentPath, "b/b/c");
        fileSys.mkdirs(testPath2);
        Path testPath3 = new Path(currentPath, "c/b/c");
        fileSys.mkdirs(testPath3);
        Path testPath4 = new Path(currentPath, "e/f/g");
        Path testPath5 = new Path(basePath, "whitelist_20220809");
        fileSys.mkdirs(testPath4);

        TrashPolicyDefault trashPolicyDefault = new TrashPolicyDefault();

        trashPolicyDefault.initialize(conf, fileSys);
        trashPolicyDefault.checkWhiteList(testPath5);
    }
}
