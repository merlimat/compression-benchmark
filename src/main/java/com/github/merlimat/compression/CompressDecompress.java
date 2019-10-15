package com.github.merlimat.compression;

import com.github.merlimat.compression.aircompressor.AirCompressorCodecLZ4;
import com.github.merlimat.compression.aircompressor.AirCompressorCodecSnappy;
import com.github.merlimat.compression.aircompressor.AirCompressorCodecZstd;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecLZ4;
import org.apache.pulsar.common.compression.CompressionCodecSnappy;
import org.apache.pulsar.common.compression.CompressionCodecZLib;
import org.apache.pulsar.common.compression.CompressionCodecZstd;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 3)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsAppend = { "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints" })
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.Throughput)
public class CompressDecompress {

    private static Map<String, CompressionCodec> providers = new HashMap<String, CompressionCodec>();

    static {
        providers.put("Zip", new CompressionCodecZLib());
        providers.put("JNI-LZ4", new CompressionCodecLZ4());
        providers.put("JNI-ZStd", new CompressionCodecZstd());
        providers.put("JNI-Snappy", new CompressionCodecSnappy());
        providers.put("AC-Snappy", new AirCompressorCodecSnappy());
        providers.put("AC-LZ4", new AirCompressorCodecLZ4());
        providers.put("AC-ZStd", new AirCompressorCodecZstd());
    }

    @State(Scope.Benchmark)
    public static class BenchState {
        @Param({
                "Zip",
                "JNI-LZ4",
                "JNI-ZStd",
                "JNI-Snappy",
                "AC-Snappy",
                "AC-LZ4",
                "AC-ZStd" })
        private String provider;

        @Param({
                "10b",
                "1kb",
                "10kb",
                "64kb"
        })
        private String size;

        private CompressionCodec codec;

        private ByteBuf uncompressedData;
        private int unconpressedSize;
        private ByteBuf compressedData;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            codec = providers.get(provider);

            try (InputStream is = CompressDecompress.class.getClassLoader()
                    .getResourceAsStream(size + "_data.json")) {
                unconpressedSize = is.available();
                byte[] targetArray = new byte[unconpressedSize];
                is.read(targetArray);

                uncompressedData = PooledByteBufAllocator.DEFAULT.directBuffer(unconpressedSize);
                uncompressedData.writeBytes(targetArray);
            }

            compressedData = codec.encode(uncompressedData);
        }

        @TearDown(Level.Trial)
        public void teardown() {
            compressedData.release();
            uncompressedData.release();
        }
    }

    @Benchmark
    public void compress(BenchState state) {
        state.codec.encode(state.uncompressedData).release();
    }

    @Benchmark
    public void decompress(BenchState state) throws IOException {
        state.codec.decode(state.compressedData, state.unconpressedSize).release();
    }

}
