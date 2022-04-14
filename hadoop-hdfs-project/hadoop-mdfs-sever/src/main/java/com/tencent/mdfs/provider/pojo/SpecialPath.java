package com.tencent.mdfs.provider.pojo;

public class SpecialPath {
    private Long id;
    private String pathPre;
    private String replacePathPre;
    private boolean isMdfs;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getPathPre() {
        return pathPre;
    }

    public void setPathPre(String pathPre) {
        this.pathPre = pathPre;
    }

    public String getReplacePathPre() {
        return replacePathPre;
    }

    public void setReplacePathPre(String replacePathPre) {
        this.replacePathPre = replacePathPre;
    }

    public boolean isMdfs() {
        return isMdfs;
    }

    public void setMdfs(boolean mdfs) {
        isMdfs = mdfs;
    }

    @Override
    public String toString() {
        return "SpecialPath{" +
                "id=" + id +
                ", pathPre='" + pathPre + '\'' +
                ", replacePathPre='" + replacePathPre + '\'' +
                ", isMdfs=" + isMdfs +
                '}';
    }
}
