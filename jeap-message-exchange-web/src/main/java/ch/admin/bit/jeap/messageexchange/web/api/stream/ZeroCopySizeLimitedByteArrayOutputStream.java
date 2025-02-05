package ch.admin.bit.jeap.messageexchange.web.api.stream;

import org.springframework.security.web.firewall.RequestRejectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

class ZeroCopySizeLimitedByteArrayOutputStream extends ByteArrayOutputStream {

    private final int maxCapacity;

    ZeroCopySizeLimitedByteArrayOutputStream(int initialCapacity, int maxCapacity) {
        super(initialCapacity);
        this.maxCapacity = maxCapacity;
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) {
        int desiredSize = size() + len;
        if (desiredSize > maxCapacity) {
            throw new RequestRejectedException("Request content exceeded limit of " + maxCapacity + " bytes");
        }
        super.write(b, off, len);
    }

    /**
     * Provides a direct reference to the underlying byte array without creating a copy
     *
     * @return An input stream for the bytes written to the byte array output stream.
     */
    public InputStream getInputStream() {
        return new ByteArrayInputStream(buf, 0, size());
    }
}
