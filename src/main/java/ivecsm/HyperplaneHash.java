package ivecsm;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Locale;
import java.util.Random;

public class HyperplaneHash {
    /**
     * An array of matrices containing all normal vectors for the planes as its rows. It has size {@code hashCount x
     * bitsPerHash x dimension }. So the first index is the ith hash. The second index selects the jth plane for the ith
     * hash. And the third index selects the kth coefficient of the normal vector of the jth plane.
     */
    protected final double[][][] planeMatrices;

    /**
     * @param hashCount   number of hashes (bands) to calculate.
     * @param bitsPerHash how many hyperplanes should be used to generate a single signature.
     * @param dimension   the dimension of the data that will be hashed.
     * @param seed        seed used for random number generation. All random planes will be the same, if the same seed
     *                    is passed.
     */
    public HyperplaneHash(int hashCount, int bitsPerHash, int dimension) {
        planeMatrices = new double[hashCount][][];
        final Random random = new Random(35L); //seed geändert
        for (int i = 0; i < planeMatrices.length; i++) {
            double[][] planeMatrix = planeMatrices[i] = new double[bitsPerHash][];
            for (int j = 0; j < planeMatrix.length; j++) {
                planeMatrix[j] = getNormal(i, j, dimension, random);
                assert planeMatrix[j].length == dimension;
            }
        }
    }

    public static double cosineSimilarity(double[] a, double[] b) {
        final double aDotB = dot(a, b);
        final double aDotA = dot(a, a);
        final double bDotB = dot(b, b);
        return aDotB / Math.sqrt(aDotA * bDotB);
    }

    public static double dot(double[] a, double[] b) {
        assert a.length == b.length;
        double res = 0.0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    //Eignene Funktionen
    public static void normalizeInPlace(double[] vector) {
        final double lenSq = dot(vector, vector);

        if (lenSq == 0.0) {
            return;
        }

        final double len = Math.sqrt(lenSq);

        for (int i = 0; i < vector.length; i++) {
            vector[i] /= len;
        }
    }

    public static double length(double[] a) {
        return Math.sqrt(dot(a, a));
    }

    public static double angle(double[] a, double[] b) {
        return Math.acos(dot(a, b) / (length(a) * length(b)));
    }

    /**
     * Generate the plane with jth index ({@code planeIndex}) for the ith ({@code bandIndex}) band. The normal vector of
     * the plane is returned. It has a sufficient length.
     * <p>
     * This implementation draws one plane from an equal distribution of all possible planes. It does not use {@code
     * bandIndex} nor {@code planeIndex} but override methods can use this to control properties of each bit of the
     * hashes.
     *
     * @param bandIndex  the index of the band.
     * @param planeIndex the index inside the band.
     * @param dimension  the number of elements in the returned array.
     * @param random     a random number generator that must be used when creating random values.
     * @return a normal vector of the plane. It should have a large enough magnitude.
     */
    protected double[] getNormal(int bandIndex, int planeIndex, int dimension, Random random) {
        return getUniformDirection(dimension, random);
    }

    /**
     * Generates a vector with sufficient magnitude drawn from a uniform direction distribution.
     * Feel free to overwrite this method if you can think of a better initialization. 
     *
     * @param dimension the dimension of the generated vector.
     * @param random    the random source used for all randomness.
     * @return a vector with dimension {@code dimension} pointing in a random direction, where each direction is equally
     * likely.
     */
    protected static double[] getUniformDirection(int dimension, Random random) {
        final double[] res = new double[dimension];
        boolean isNotAPlane = true;
        // try until we found a normal vector that is not only zeros.
        do {
            for (int k = 0; k < res.length; k++) {
                double next = res[k] = random.nextGaussian();
                if (Math.abs(next) > 1E-10) {
                    isNotAPlane = false;
                }
            }
        } while (isNotAPlane);
        return res;
    }

    /**
     * Calculates the hashes for one specific vector ({@code data}). The resulting array contains exactly {@code
     * hashCount} {@link BitSet} representing each individual hash. The first {@code bitsPerHash} bits in the bit set
     * composes the hash.
     *
     * @param data the vector to hash. Its length must be equal to the {@code dimension} value specified when creating
     *             this {@link RandomHyperplaneHash}.
     * @return the individual hashes.
     */
    public BitSet[] hash(double[] data) {
        final BitSet[] res = new BitSet[planeMatrices.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = hashI(data, i);
        }
        return res;
    }

    /**
     * Calculate the ith hash value for one specific vector ({@code data}). The first {@code bitsPerHash} bits in the
     * bit set composes the hash.
     *
     * @param data the vector to hash. Its length must be equal to the {@code dimension} value specified when creating
     *             this {@link RandomHyperplaneHash}.
     * @param i    the number of the hash that should be calculated. Must be from {@code [0, hashCount)}
     * @return the specific hash.
     */
    public BitSet hashI(double[] data, int i) {
        final double[][] planeMatrix = planeMatrices[i];
        final BitSet res = new BitSet(planeMatrix.length);
        for (int j = 0; j < planeMatrix.length; j++) {
            if (HyperplaneHash.dot(data, planeMatrix[j]) > 0.0) {
                res.set(j);
            }
        }
        return res;
    }

    
    /// EXAMPLE
    

	static String color[] = {"red","blue","green"};
	
	public static void main(String[] args) {
		
		Locale.setDefault(Locale.US);

		{ // 2d example
			HyperplaneHash rhh = new HyperplaneHash(2, 3, 2);

			System.out.println(rhh);

			System.out.println(Arrays.toString(rhh.hash(new double[] { 3, 3 })));
			System.out.println(Arrays.toString(rhh.hash(new double[] { -3, 3 })));
			System.out.println(Arrays.toString(rhh.hash(new double[] { -3, -3 })));
			System.out.println(Arrays.toString(rhh.hash(new double[] { 3, -3 })));
			
			System.out.println("R code:");
			System.out.println("plot(c(3,3,-3,-3),c(-3,3,3,-3),type='p',xlim=c(-6,6),ylim=c(-6,6));");
			double[][] firstband = rhh.planeMatrices[0];
			for (int i=0;i<firstband.length;++i) {
				double a = firstband[i][0];
				double b = firstband[i][1];
				int lty = (b>0)?1:2; // gestrichelt: unterhalb, durchgezogen: oberhalb
				System.out.printf("abline(b=%6f,a=0,col='%s',lty=%d)\n", -a/b, color[i],lty);
			}
			
		}

	}
    
}
