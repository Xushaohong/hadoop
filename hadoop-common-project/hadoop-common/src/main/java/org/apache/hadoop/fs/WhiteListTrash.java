package org.apache.hadoop.fs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_WHITE_LIST;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_WHITE_LIST_PREFIX;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_WHITE_LIST_PREFIX_DEFAULT;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.fs.Path.SEPARATOR_CHAR;

/**
 * Class to protect white list in trash
 */
public class WhiteListTrash {

    private static final String BACK_DIR = "bak_";
    private static final Path CURRENT = new Path("Current");
    private static final DateFormat WHITE_LIST_FORMAT = new SimpleDateFormat("yyyyMMdd");
    private static final FsPermission PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

    private static final Logger LOG =
            LoggerFactory.getLogger(WhiteListTrash.class);

    private final Path subTrashPath;
    private final FileSystem fs;
    private final String whiteTrashPrefix;
    private final String whiteListConfFile;

    // extract from sub trash path;
    private Path baseTrashPath;
    private Path relativePtah;
    private Path middlePath;

    /**
     * sub path from trash path
     * @param subTrashPath
     */
    public WhiteListTrash(Configuration conf, FileSystem fs, Path subTrashPath) {
        this.subTrashPath = subTrashPath;
        this.fs = fs;
        this.whiteTrashPrefix = conf.get(FS_TRASH_WHITE_LIST_PREFIX, FS_TRASH_WHITE_LIST_PREFIX_DEFAULT);
        this.whiteListConfFile = conf.get(FS_TRASH_WHITE_LIST);
    }


    /**
     *  If subTrashPath is valid, return true, else return false.
     * @return
     */
    public boolean extractInfoFromTrashPath() {
        String subTrashPathStr = Path.getPathWithoutSchemeAndAuthority(subTrashPath).toString();
        String[] tmpStrList = StringUtils.split(subTrashPathStr, SEPARATOR_CHAR);
        if (tmpStrList == null || tmpStrList.length < 1) {
            return false;
        }
        boolean findTrashPrefix = false;
        int index = -1;
        for(String pathName : tmpStrList) {
            index += 1;
            if (TRASH_PREFIX.equals(pathName)) {
                findTrashPrefix = true;
                break;
            }
        }

        // if find .Trash in path, split it.
        if (findTrashPrefix && index < tmpStrList.length) {
            String[] basePathList = new String[index + 1];

            System.arraycopy(tmpStrList, 0, basePathList, 0, index + 1);
            this.baseTrashPath = new Path(SEPARATOR_CHAR + StringUtils.join(basePathList, SEPARATOR_CHAR));

            if (index + 1 < tmpStrList.length) {
                this.middlePath = new Path(tmpStrList[index + 1]);
                String[] relativePathList = new String[tmpStrList.length - index - 2];
                if (relativePathList.length > 0) {
                    System.arraycopy(tmpStrList, index + 2, relativePathList, 0, relativePathList.length);
                    this.relativePtah = new Path(SEPARATOR_CHAR + StringUtils.join(relativePathList, SEPARATOR_CHAR));
                }
            }
            LOG.debug("{} -> {} -> {}", baseTrashPath, middlePath, relativePtah);
            return true;
        }
        return false;
    }

    /**
     * move old white trash to current path
     * @param protectedWhitePaths
     */
    public void moveOldWhiteTrash(Set<String> protectedWhitePaths) {
        try {
            List<String> bakNameList = new ArrayList<>();
            FileStatus[] trashChildPaths = fs.listStatus(getBaseTrashPath(), path -> {
                String pathName = path.getName();
                LOG.debug("path name is " + pathName);
                if (pathName != null && pathName.startsWith(BACK_DIR)) {
                    bakNameList.add(pathName);
                }
                return pathName != null && pathName.startsWith(whiteTrashPrefix)
                        && !protectedWhitePaths.contains(pathName);
            });
            String oldPath = null;
            for (String bakName : bakNameList) {
                if (StringUtils.compare(oldPath, bakName) < 0) {
                    oldPath = bakName;
                }
            }
            if (oldPath == null) {
                oldPath = CURRENT.getName();
            }
            Path trashCurrent = new Path(getBaseTrashPath(), oldPath);
            for (FileStatus fileStatus : trashChildPaths) {
                // move back to current or old bak directory
                String pathName = fileStatus.getPath().getName();
                Path trashPath = new Path(trashCurrent, pathName);
                Path trashPathParent = trashPath.getParent();
                if (fs.mkdirs(trashPathParent, PERMISSION)) {
                    LOG.debug("start rename " + fileStatus.getPath() + " to " + trashPath);
                    fs.rename(fileStatus.getPath(), trashPath, Options.Rename.TO_TRASH);
                }
            }
        } catch (Exception ex) {
            LOG.debug("moveOldWhiteTrash got exception", ex);
        }
    }

    /**
     * Get file path list from white list.
     * @return list of paths
     */
    private List<String> getWhiteList() {
        try {
            return FileUtils.readLines(Paths.get(whiteListConfFile).toFile(), StandardCharsets.UTF_8);
        } catch (Exception ex) {
            LOG.error("Exception while reading {}", whiteListConfFile, ex);
        }
        return null;
    }

    /**
     * rename path to target, if target exists, add with timestamp
     */
    private void renamePathOrWithTimestamp(Path sourcePath, Path targetPath, boolean isTargetExist) throws IOException {
        if (!isTargetExist) {
            Path targetPathParent = targetPath.getParent();
            fs.mkdirs(targetPathParent, PERMISSION);
        } else {
            targetPath = new Path(targetPath.toString() + Time.now());
        }
        LOG.debug("Rename {} -> {}", sourcePath, targetPath);
        fs.rename(sourcePath, targetPath);
    }

    private void recursiveMoveProtectedPath(Path sourcePath, Path targetPath) {
        try {
            // if target path not exists, move directly.
            if (!fs.exists(targetPath)) {
                renamePathOrWithTimestamp(sourcePath, targetPath, false);
            } else {
                FileStatus sourceStatus = fs.getFileStatus(sourcePath);
                if (sourceStatus.isDirectory()) {
                    FileStatus[] childList = fs.listStatus(sourcePath);
                    // no child
                    if (childList == null || childList.length < 1) {
                        renamePathOrWithTimestamp(sourcePath, targetPath, true);
                    } else {
                        for (FileStatus child : childList) {
                            Path childPath = child.getPath();
                            recursiveMoveProtectedPath(childPath, new Path(targetPath, childPath.getName()));
                        }
                    }
                } else {
                    // just try to rename it to target
                    renamePathOrWithTimestamp(sourcePath, targetPath, true);
                }
            }
        } catch (Exception ex) {
            LOG.debug("recursiveMoveProtectedPath got exception", ex);
        }
    }


    /**
     * protect white list paths.
     */
    public boolean protectWhiteList() {
        boolean isProtected = false;
        try {

            if (middlePath == null) {
                middlePath = CURRENT;
            }
            if (relativePtah == null) {
                relativePtah = new Path("/");
            }

            // start to protect delete path
            List<String> whiteListPaths = getWhiteList();
            List<String> pendingWhiteList = new ArrayList<>();

            if (whiteListPaths != null) {
                for (String whitePathStr : whiteListPaths) {
                    if (whitePathStr.startsWith(relativePtah.toString())) {
                        pendingWhiteList.add(whitePathStr);
                    }
                    if (relativePtah.toString().startsWith(whitePathStr)) {
                        isProtected = true;
                        pendingWhiteList.add(whitePathStr);
                    }
                }
                for (String whitePathStr : pendingWhiteList) {
                    Path sourceBase = new Path(baseTrashPath, middlePath);
                    Path whitePath = Path.mergePaths(sourceBase, new Path(whitePathStr));
                    Path targetBase = new Path(baseTrashPath,
                            whiteTrashPrefix + WHITE_LIST_FORMAT.format(new Date(Time.now())));
                    if (fs.exists(whitePath)) {
                        recursiveMoveProtectedPath(whitePath, Path.mergePaths(targetBase, new Path(whitePathStr)));
                    }
                }
            }
        } catch (Exception ex) {
            LOG.debug("protectWhiteList got exception", ex);
        }
        return isProtected;
    }


    public Path getBaseTrashPath() {
        return baseTrashPath;
    }
}
