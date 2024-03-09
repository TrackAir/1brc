/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class TrackMMap {
    private static final String FILE = "./measurements.txt";
    private static long fileSize ;
    private static final Map<String, StationStats> statsMap = new HashMap<>();
    public static void main(String[] args) throws IOException {
        var clockStart = System.currentTimeMillis();
        calculateTrack();
        System.err.format("Took %,d ms\n", System.currentTimeMillis() - clockStart);
    }

    public static void calculateTrack() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r");
             ) {
            long length = raf.length();
            MemorySegment chunk = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length, Arena.global());
            for (long cursor = 0 ; cursor < chunk.byteSize() ; ){
                long semicolonPos = findByte(cursor, ';', chunk);
                long endPos = findByte(semicolonPos+1, '\n', chunk);
                String name = stringAt(cursor, semicolonPos, chunk);
//                String num = stringAt(semicolonPos+1, endPos, chunk);
//                int numDoub = Integer.parseInt(num)
                int numDoub = parseTemperature(semicolonPos,chunk);
                StationStats stationStats = statsMap.computeIfAbsent(name, key -> new StationStats(name));
                stationStats.min = Math.min(stationStats.min, numDoub);
                stationStats.max = Math.min(stationStats.max, numDoub);
                stationStats.sum += numDoub;
                stationStats.count += 1;
                cursor = endPos + 1;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static int parseTemperature(long semicolonPos, MemorySegment chunk) {
        long off = semicolonPos + 1;
        int sign = 1;
        byte b = chunk.get(JAVA_BYTE, off++);
        if (b == '-') {
            sign = -1;
            b = chunk.get(JAVA_BYTE, off++);
        }
        int temp = b - '0';
        b = chunk.get(JAVA_BYTE, off++);
        if (b != '.') {
            temp = 10 * temp + b - '0';
            // we found two integer digits. The next char is definitely '.', skip it:
            off++;
        }
        b = chunk.get(JAVA_BYTE, off);
        temp = 10 * temp + b - '0';
        return sign * temp;
    }
    private static long findByte(long cursor, int b, MemorySegment chunk) {
        for (var i = cursor; i < chunk.byteSize(); i++) {
            if (chunk.get(JAVA_BYTE, i) == b) {
                return i;
            }
        }
        throw new RuntimeException(((char) b) + " not found");
    }

    private static String stringAt(long start, long limit, MemorySegment chunk) {
        return new String(
                chunk.asSlice(start, limit - start).toArray(JAVA_BYTE),
                StandardCharsets.UTF_8
        );
    }


    static class StationStats implements Comparable<StationStats> {
        String name;
        long sum;
        int count;
        int min;
        int max;

        StationStats(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }

        @Override
        public boolean equals(Object that) {
            return that.getClass() == StationStats.class && ((StationStats) that).name.equals(this.name);
        }

        @Override
        public int compareTo(StationStats that) {
            return name.compareTo(that.name);
        }
    }

}
