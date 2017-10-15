package org.embulk.output.orc;

public enum OrcCodec
{
    ZLIB("zlib"),
    SNAPPY("snappy"),
    LZO("lzo"),
    LZ4("lz4"),
    NONE("none"),;
    String kind;

    OrcCodec(String kind)
    {
        this.kind = kind;
    }

    public String getKind()
    {
        return kind;
    }
}
