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
import java.util.stream.Collectors;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class TrackThread {
    private static final String FILE = "./measurements.txt";
    private static final Map<String, StationStats> map = new HashMap<>();



    public static void main(String[] args) throws IOException {
        var clockStart = System.currentTimeMillis();
        calculateTrack();
        System.err.format("Took %,d ms\n", System.currentTimeMillis() - clockStart);
    }

    public static void calculateTrack() throws IOException {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r");
             ) {
            long length = raf.length();
            MemorySegment chunk = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length, Arena.global());
            Thread[] threads = new Thread[availableProcessors];
            Compute[] computes = new Compute[availableProcessors];
            long cursor = 0L;
            long size = length / availableProcessors;
            for (int i = 0 ; i < availableProcessors ; i++){
                long curCursor = cursor;
                long curSize = size;
                MemorySegment slice;
                if ( i != availableProcessors-1){
                    while(chunk.get(JAVA_BYTE, curSize+curCursor) != '\n'){
                        curSize++;
                    }
                    slice = chunk.asSlice(curCursor, curSize+1);
                    cursor = curSize  + 1 +  cursor;
                }else {
                    slice = chunk.asSlice(curCursor, chunk.byteSize() - curCursor);
                }
                Compute compute = new Compute(slice);
                Thread thread = new Thread(compute);
                threads[i] = thread;
                computes[i] = compute;
            }

            for (int i = 0 ; i < availableProcessors ; i++){
                threads[i].start();
            }
            for (int i = 0 ; i < availableProcessors ; i++){
                threads[i].join();
            }

            for (int i = 0 ; i < availableProcessors ; i++){
                Map<String, StationStats> statsMap = computes[i].statsMap;
                for (Map.Entry<String, StationStats> entry : statsMap.entrySet()){
                    String key = entry.getKey();
                    StationStats value = entry.getValue();
                    StationStats stationStats = map.computeIfAbsent(key, k -> new StationStats(key));
                    stationStats.min = Math.min(stationStats.min, value.min);
                    stationStats.max = Math.max(stationStats.max, value.max);
                    stationStats.count = stationStats.count+value.count;
                    stationStats.sum = stationStats.sum+value.sum;
                }
            }

            System.out.print("{");
            System.out.print(
                    map.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
            System.out.println("}");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Compute implements Runnable{
        MemorySegment chunk;

        long cursor;
        Map<String, StationStats> statsMap = new HashMap<>();

        public Compute(MemorySegment chunk){
            this.chunk = chunk;
        }

        @Override
        public void run() {
            for (long cursor = 0 ; cursor < chunk.byteSize() ; ){
                long semicolonPos = findByte(cursor, ';', chunk);
                long endPos = findByte(semicolonPos+1, '\n', chunk);
                String name = stringAt(cursor, semicolonPos, chunk);
                int numDoub = parseTemperature(semicolonPos,chunk);
                StationStats stationStats = statsMap.computeIfAbsent(name, key -> new StationStats(name));
                stationStats.min = Math.min(stationStats.min, numDoub);
                stationStats.max = Math.max(stationStats.max, numDoub);
                stationStats.sum += numDoub;
                stationStats.count += 1;
                cursor = endPos + 1;
            }
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
