package io.minio.spark.select.util;


import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A URI wrapper that can parse out information about an S3 URI.
 */

public class COSURI {
    private static final Pattern ENDPOINT_PATTERN =
            Pattern.compile("^(.+\\.)?cos[.-]([a-z0-9-]+)\\.");

    private static final Pattern VERSION_ID_PATTERN = Pattern.compile("[&;]");

    private final URI uri;

    private final boolean isPathStyle;
    private final String bucket;
    private final String key;
    private final String versionId;
    private final String region;

    public COSURI(final String str) {
        this(str, true);
    }

    public COSURI(final String str, final boolean urlEncode) {
        this(URI.create(preprocessUrlStr(str, urlEncode)), urlEncode);
    }

    public COSURI(final URI uri) {
        this(uri, false);
    }

    private COSURI(final URI uri, final boolean urlEncode) {
        if (uri == null) {
            throw new IllegalArgumentException("uri cannot be null");
        }
        this.uri = uri;

        // cos://*
        if ("cos".equalsIgnoreCase(uri.getScheme())) {
            this.region = null;
            this.versionId = null;
            this.isPathStyle = false;
            this.bucket = uri.getAuthority();

            if (bucket == null) {
                throw new IllegalArgumentException("Invalid COS URI: no bucket: "
                        + uri);
            }

            String path = uri.getPath();
            if (path.length() <= 1) {
                // cos://bucket or cos://bucket/
                this.key = null;
            } else {
                // cos://bucket/key
                // Remove the leading '/'.
                this.key = uri.getPath().substring(1);
            }
            return;
        }

        String host = uri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("Invalid COS URI: no hostname: "
                    + uri);
        }

        Matcher matcher = ENDPOINT_PATTERN.matcher(host);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid COS URI: hostname does not appear to be a valid S3 "
                            + "endpoint: " + uri);
        }

        String prefix = matcher.group(1);
        if (prefix == null || prefix.isEmpty()) {

            // No bucket name in the authority; parse it from the path.
            this.isPathStyle = true;

            // Use the raw path to avoid running afoul of '/'s in the
            // bucket name if we have not performed full URL encoding
            String path = urlEncode ? uri.getPath() : uri.getRawPath();

            if ("/".equals(path)) {
                this.bucket = null;
                this.key = null;
            } else {

                int index = path.indexOf('/', 1);
                if (index == -1) {

                    // https://s3.amazonaws.com/bucket
                    this.bucket = decode(path.substring(1));
                    this.key = null;

                } else if (index == (path.length() - 1)) {

                    // https://s3.amazonaws.com/bucket/
                    this.bucket = decode(path.substring(1, index));
                    this.key = null;

                } else {

                    // https://s3.amazonaws.com/bucket/key
                    this.bucket = decode(path.substring(1, index));
                    this.key = decode(path.substring(index + 1));

                }
            }

        } else {

            // Bucket name was found in the host; path is the key.
            this.isPathStyle = false;

            // Remove the trailing '.' from the prefix to get the bucket.
            this.bucket = prefix.substring(0, prefix.length() - 1);

            String path = uri.getPath();
            if (path == null || path.isEmpty() || "/".equals(uri.getPath())) {
                this.key = null;
            } else {
                // Remove the leading '/'.
                this.key = uri.getPath().substring(1);
            }
        }

        this.versionId = parseVersionId(uri.getRawQuery());

        if ("amazonaws".equals(matcher.group(2))) {
            // No region specified
            this.region = null;
        } else {
            this.region = matcher.group(2);
        }
    }

    private static String parseVersionId(String query) {
        if (query != null) {
            String[] params = VERSION_ID_PATTERN.split(query);
            for (String param : params) {
                if (param.startsWith("versionId=")) {
                    return decode(param.substring(10));
                }
            }
        }
        return null;
    }

    public URI getURI() {
        return uri;
    }

    public boolean isPathStyle() {
        return isPathStyle;
    }


    public String getBucket() {
        return bucket;
    }


    public String getKey() {
        return key;
    }


    public String getRegion() {
        return region;
    }

    @Override
    public String toString() {
        return uri.toString();
    }

    private static String preprocessUrlStr(final String str, final boolean encode) {
        if (encode) {
            try {
                return (URLEncoder.encode(str, "UTF-8")
                        .replace("%3A", ":")
                        .replace("%2F", "/")
                        .replace("+", "%20"));
            } catch (UnsupportedEncodingException e) {
                // This should never happen unless there is something
                // fundamentally broken with the running JVM.
                throw new RuntimeException(e);
            }
        }
        return str;
    }


    private static String decode(final String str) {
        if (str == null) {
            return null;
        }

        for (int i = 0; i < str.length(); ++i) {
            if (str.charAt(i) == '%') {
                return decode(str, i);
            }
        }

        return str;
    }


    private static String decode(final String str, final int firstPercent) {
        StringBuilder builder = new StringBuilder();
        builder.append(str.substring(0, firstPercent));

        appendDecoded(builder, str, firstPercent);

        for (int i = firstPercent + 3; i < str.length(); ++i) {
            if (str.charAt(i) == '%') {
                appendDecoded(builder, str, i);
                i += 2;
            } else {
                builder.append(str.charAt(i));
            }
        }

        return builder.toString();
    }


    private static void appendDecoded(final StringBuilder builder,
                                      final String str,
                                      final int index) {

        if (index > str.length() - 3) {
            throw new IllegalStateException("Invalid percent-encoded string:"
                    + "\"" + str + "\".");
        }

        char first = str.charAt(index + 1);
        char second = str.charAt(index + 2);

        char decoded = (char) ((fromHex(first) << 4) | fromHex(second));
        builder.append(decoded);
    }


    private static int fromHex(final char c) {
        if (c < '0') {
            throw new IllegalStateException(
                    "Invalid percent-encoded string: bad character '" + c + "' in "
                            + "escape sequence.");
        }
        if (c <= '9') {
            return (c - '0');
        }

        if (c < 'A') {
            throw new IllegalStateException(
                    "Invalid percent-encoded string: bad character '" + c + "' in "
                            + "escape sequence.");
        }
        if (c <= 'F') {
            return (c - 'A') + 10;
        }

        if (c < 'a') {
            throw new IllegalStateException(
                    "Invalid percent-encoded string: bad character '" + c + "' in "
                            + "escape sequence.");
        }
        if (c <= 'f') {
            return (c - 'a') + 10;
        }

        throw new IllegalStateException(
                "Invalid percent-encoded string: bad character '" + c + "' in "
                        + "escape sequence.");
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        COSURI that = (COSURI) o;

        if (isPathStyle != that.isPathStyle) return false;
        if (!uri.equals(that.uri)) return false;
        if (bucket != null ? !bucket.equals(that.bucket) : that.bucket != null) return false;
        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (versionId != null ? !versionId.equals(that.versionId) : that.versionId != null) return false;
        return region != null ? region.equals(that.region) : that.region == null;
    }

    public int hashCode() {
        int result = uri.hashCode();
        result = 31 * result + (isPathStyle ? 1 : 0);
        result = 31 * result + (bucket != null ? bucket.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (versionId != null ? versionId.hashCode() : 0);
        result = 31 * result + (region != null ? region.hashCode() : 0);
        return result;
    }


}
