package com.aliyun.odps.spark.examples.simhash;

import java.math.BigInteger;
import java.util.StringTokenizer;

public class SimHash {

    /**
     *
     * @param tokens
     * @param hashBits
     * @param radix
     * @return
     */
    public static String simHash(String tokens,int hashBits,int radix) {
        return simHash(tokens,hashBits).toString(radix);
    }

    /**
     *
     * @param tokens
     * @param hashBits
     * @return
     */
    public static BigInteger simHash(String tokens,int hashBits) {
        int[] v = new int[hashBits];
        StringTokenizer stringTokens = new StringTokenizer(tokens);
        while (stringTokens.hasMoreTokens()) {
            String temp = stringTokens.nextToken();
            BigInteger t = hash(temp,hashBits);
            for (int i = 0; i < hashBits; i++) {
                BigInteger bitmask = new BigInteger("1").shiftLeft(i);
                if (t.and(bitmask).signum() != 0) {
                    v[i] += 1;
                } else {
                    v[i] -= 1;
                }
            }
        }
        BigInteger fingerprint = new BigInteger("0");
        for (int i = 0; i < hashBits; i++) {
            if (v[i] >= 0) {
                fingerprint = fingerprint.add(new BigInteger("1").shiftLeft(i));
            }
        }
        return fingerprint;
    }

    private static BigInteger hash(String source,int hashbits) {
        if (source == null || source.length() == 0) {
            return new BigInteger("0");
        } else {
            char[] sourceArray = source.toCharArray();
            BigInteger x = BigInteger.valueOf(((long)sourceArray[0]) << 7);
            BigInteger m = new BigInteger("1000003");
            BigInteger mask = new BigInteger("2").pow(hashbits).subtract(
                new BigInteger("1"));
            for (char item : sourceArray) {
                BigInteger temp = BigInteger.valueOf((long)item);
                x = x.multiply(m).xor(temp).and(mask);
            }
            x = x.xor(new BigInteger(String.valueOf(source.length())));
            if (x.equals(new BigInteger("-1"))) {
                x = new BigInteger("-2");
            }
            return x;
        }
    }

    public static int hammingDistance(BigInteger hash1,BigInteger hash2,int hashBits) {
        BigInteger m = new BigInteger("1").shiftLeft(hashBits).subtract(
            new BigInteger("1"));
        BigInteger x = hash1.xor(hash2).and(m);
        int tot = 0;
        while (x.signum() != 0) {
            tot += 1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }

    public static void main(String[] args) {
        String s = "This is a test string for testing";
        BigInteger hash1 = SimHash.simHash(s, 128);
        System.out.println(hash1 + "  " + hash1.bitLength());
        System.out.println(hash1.toString(2) + "  " + hash1.bitLength());

        s = "This is a test string for testing also";
        BigInteger hash2 = SimHash.simHash(s, 128);
        System.out.println(hash2 + "  " + hash2.bitCount());
        System.out.println(hash2.toString(2) + "  " + hash1.bitLength());

        s = "This is a test string for testing als";
        BigInteger hash3 = SimHash.simHash(s, 128);
        System.out.println(hash3 + "  " + hash3.bitCount());
        System.out.println(hash3.toString(2) + "  " + hash3.bitCount());

        s = "a b c d ";
        BigInteger hash4 = SimHash.simHash(s, 128);
        System.out.println(hash4 + "  " + hash4.bitCount());
        System.out.println(hash4.toString(2) + "  " + hash4.bitCount());

        s = "a b c";
        BigInteger hash5 = SimHash.simHash(s, 128);
        System.out.println(hash5 + "  " + hash5.bitCount());
        System.out.println(hash5.toString(2) + "  " + hash5.bitCount());

        System.out.println("============================");
        System.out.println(SimHash.hammingDistance(hash1,hash2,128));
        System.out.println(SimHash.hammingDistance(hash1,hash3,128));
        System.out.println(SimHash.hammingDistance(hash4,hash5,128));


    }
}