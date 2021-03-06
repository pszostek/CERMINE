/**
 * This file is part of CERMINE project.
 * Copyright (c) 2011-2013 ICM-UW
 *
 * CERMINE is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CERMINE is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with CERMINE. If not, see <http://www.gnu.org/licenses/>.
 */

package pl.edu.icm.cermine.structure.tools;

import java.util.*;
import pl.edu.icm.cermine.structure.model.*;

/**
 *
 * @author krusek
 */
public final class BxModelUtils {
    
    private static final double SIMILARITY_TOLERANCE = 0.001;
    
    private BxModelUtils() {}
    
	public static void setParents(BxDocument doc) {
		for(BxPage page: doc.getPages()) {
			page.setParent(doc);
			setParents(page);
		}
	}
	
	public static void setParents(BxPage page) {
		for(BxZone zone: page.getZones()) {
			for(BxLine line: zone.getLines()) {
				for(BxWord word: line.getWords()) {
					for(BxChunk chunk: word.getChunks()) {
						chunk.setParent(word);
                    }
					word.setParent(line);
				}
				line.setParent(zone);
			}
			zone.setParent(page);
		}
	}
	
    public static void sortZonesYX(BxPage page, final double tolerance) {
        Collections.sort(page.getZones(), new Comparator<BxZone>() {

            @Override
            public int compare(BxZone o1, BxZone o2) {
                int cmp = 0;
                if (Math.abs(o1.getBounds().getY() - o2.getBounds().getY()) > tolerance) {
                    cmp = Double.compare(o1.getBounds().getY(), o2.getBounds().getY());
                }
                if (cmp != 0) {
                    return cmp;
                } else {
                    return Double.compare(o1.getBounds().getX(), o2.getBounds().getX());
                }
            }
        });
    }

    public static void sortZonesYX(BxPage page) {
        sortZonesYX(page, 0);
    }

    public static void sortZonesXY(BxPage page, final double tolerance) {
        Collections.sort(page.getZones(), new Comparator<BxZone>() {

            @Override
            public int compare(BxZone o1, BxZone o2) {
                int cmp = 0;
                if (Math.abs(o1.getBounds().getX() - o2.getBounds().getX()) > tolerance) {
                    cmp = Double.compare(o1.getBounds().getX(), o2.getBounds().getX());
                }
                if (cmp != 0) {
                    return cmp;
                } else {
                    return Double.compare(o1.getBounds().getY(), o2.getBounds().getY());
                }
            }
        });
    }

    public static void sortZonesXY(BxPage page) {
        sortZonesXY(page, 0);
    }

    public static void sortLines(BxZone zone) {
        Collections.sort(zone.getLines(), new Comparator<BxLine>() {

            @Override
            public int compare(BxLine o1, BxLine o2) {
                int cmp = Double.compare(o1.getBounds().getY(), o2.getBounds().getY());
                if (cmp == 0) {
                    cmp = Double.compare(o1.getBounds().getX(), o2.getBounds().getX());
                }
                return cmp;
            }
        });
    }

    public static void sortWords(BxLine line) {
        Collections.sort(line.getWords(), new Comparator<BxWord>() {

            @Override
            public int compare(BxWord o1, BxWord o2) {
                return Double.compare(o1.getBounds().getX(), o2.getBounds().getX());
            }
        });
    }

    public static void sortChunks(BxWord word) {
        Collections.sort(word.getChunks(), new Comparator<BxChunk>() {

            @Override
            public int compare(BxChunk o1, BxChunk o2) {
                return Double.compare(o1.getBounds().getX(), o2.getBounds().getX());
            }
        });
    }

    public static void sortZoneRecursively(BxZone zone) {
        sortLines(zone);
        for (BxLine line : zone.getLines()) {
            sortWords(line);
            for (BxWord word : line.getWords()) {
                sortChunks(word);
            }
        }
    }

    public static void sortZonesRecursively(BxPage page) {
        for (BxZone zone : page.getZones()) {
            sortZoneRecursively(zone);
        }
    }

    public static void sortZonesRecursively(BxDocument document) {
        for (BxPage page : document.getPages()) {
            sortZonesRecursively(page);
        }
    }

    /**
     * Creates a deep copy of the word.
     *
     * @param zone
     * @return
     */
    public static BxWord deepClone(BxWord word) {
        BxWord copy = new BxWord().setBounds(word.getBounds());
        for (BxChunk chunk : word.getChunks()) {
        	BxChunk copiedChunk = deepClone(chunk);
        	copiedChunk.setParent(copy);
        	copy.addChunk(chunk);
        }
        return copy;
    }

    /**
     * Creates a deep copy of the line.
     *
     * @param zone
     * @return
     */
    public static BxLine deepClone(BxLine line) {
        BxLine copy = new BxLine().setBounds(line.getBounds());
        for (BxWord word : line.getWords()) {
        	BxWord copiedWord = deepClone(word);
        	copiedWord.setParent(copy);
            copy.addWord(copiedWord);
        }
        return copy;
    }

    public static BxChunk deepClone(BxChunk chunk) {
    	return new BxChunk(chunk.getBounds(), chunk.toText());
    }
    
    /**
     * Creates a deep copy of the zone.
     *
     * @param zone
     * @return
     */

    public static BxZone deepClone(BxZone zone) {
        BxZone copy = new BxZone().setLabel(zone.getLabel()).setBounds(zone.getBounds());
        for (BxLine line : zone.getLines()) {
        	BxLine copiedLine = deepClone(line);
        	copiedLine.setParent(copy);
            copy.addLine(copiedLine);
        }
        for (BxChunk chunk : zone.getChunks()) {
            copy.addChunk(chunk);
        }
        return copy;
    }

    /**
     * Creates a deep copy of the page.
     * 
     * @param page
     * @return
     */
    public static BxPage deepClone(BxPage page) {
        BxPage copy = new BxPage().setBounds(page.getBounds());
        for (BxZone zone : page.getZones()) {
        	BxZone copiedZone = deepClone(zone);
        	copiedZone.setParent(copy);
            copy.addZone(copiedZone);
        }
        for (BxChunk chunk : page.getChunks()) {
            copy.addChunk(chunk);
        }
        return copy;
    }

    /**
     * Creates a deep copy of the document.
     *
     * @param document
     * @return
     */
    public static BxDocument deepClone(BxDocument document) {
        BxDocument copy = new BxDocument();
        copy.setFilename(document.getFilename());
        for (BxPage page : document.getPages()) {
        	BxPage copiedPage = deepClone(page);
        	copiedPage.setParent(copy);
            copy.addPage(copiedPage);
        }
        return copy;
    }

    public static List<BxDocument> deepClone(List<BxDocument> documents) {
    	List<BxDocument> copy = new ArrayList<BxDocument>(documents.size());
    	for(BxDocument doc: documents) {
    		copy.add(deepClone(doc));
    	}
    	return copy;
    }

    /**
     * Maps segmented chunks to words which they belong to.
     *
     * @param page
     * @return
     */
    public static Map<BxChunk, BxWord> mapChunksToWords(BxPage page) {
        Map<BxChunk, BxWord> map = new HashMap<BxChunk, BxWord>();
        for (BxZone zone : page.getZones()) {
            for (BxLine line : zone.getLines()) {
                for (BxWord word : line.getWords()) {
                    for (BxChunk chunk : word.getChunks()) {
                        map.put(chunk, word);
                    }
                }
            }
        }
        return map;
    }

    /**
     * Maps segmented chunks to lines which they belong to.
     *
     * @param page
     * @return
     */
    public static Map<BxChunk, BxLine> mapChunksToLines(BxPage page) {
        Map<BxChunk, BxLine> map = new HashMap<BxChunk, BxLine>();
        for (BxZone zone : page.getZones()) {
            for (BxLine line : zone.getLines()) {
                for (BxWord word : line.getWords()) {
                    for (BxChunk chunk : word.getChunks()) {
                        map.put(chunk, line);
                    }
                }
            }
        }
        return map;
    }

    /**
     * Maps segmented chunks to zones which they belong to.
     *
     * @param page page containing zones
     * @return
     */
    public static Map<BxChunk, BxZone> mapChunksToZones(BxPage page) {
        Map<BxChunk, BxZone> map = new HashMap<BxChunk, BxZone>();
        for (BxZone zone : page.getZones()) {
            for (BxLine line : zone.getLines()) {
                for (BxWord word : line.getWords()) {
                    for (BxChunk chunk : word.getChunks()) {
                        map.put(chunk, zone);
                    }
                }
            }
        }
        return map;
    }

    /**
     * Counts segmented chunks in line.
     *
     * @param line
     * @return number of segmented chunks
     */
    public static int countChunks(BxLine line) {
        int chunkCount = 0;
        for (BxWord word : line.getWords()) {
            chunkCount += word.getChunks().size();
        }
        return chunkCount;
    }

    /**
     * Counts segmented chunks in zone.
     *
     * @param zone
     * @return number of segmented chunks
     */
    public static int countChunks(BxZone zone) {
        int chunkCount = 0;
        for (BxLine line : zone.getLines()) {
            for (BxWord word : line.getWords()) {
                chunkCount += word.getChunks().size();
            }
        }
        return chunkCount;
    }

    public static boolean contains(BxBounds containing, BxBounds contained, double tolerance) {
        return containing.getX() <= contained.getX() + tolerance
                && containing.getY() <= contained.getY() + tolerance
                && containing.getX() + containing.getWidth() >= contained.getX() + contained.getWidth() - tolerance
                && containing.getY() + containing.getHeight() >= contained.getY() + contained.getHeight() - tolerance;
    }
    
    public static boolean areEqual(BxChunk chunk1, BxChunk chunk2) {
        if (!chunk1.toText().equals(chunk2.toText())) {
            return false;
        }
        if (!chunk1.getBounds().isSimilarTo(chunk2.getBounds(), SIMILARITY_TOLERANCE)) {
            return false;
        }
        return true;
    }
    
    public static boolean areEqual(BxWord word1, BxWord word2) {
        if (word1.getChunks().size() != word2.getChunks().size()) {
			return false;
        }
		for (int i = 0; i < word1.getChunks().size(); i++) {
            if (!areEqual(word1.getChunks().get(i), word2.getChunks().get(i))) {
                return false;
            }
        }
        return true;
    }
    
    public static boolean areEqual(BxLine line1, BxLine line2) {
        if (line1.getWords().size() != line2.getWords().size()) {
			return false;
        }
		for (int i = 0; i < line1.getWords().size(); i++) {
            if (!areEqual(line1.getWords().get(i), line2.getWords().get(i))) {
                return false;
            }
        }
        return true;
    }
    
    public static boolean areEqual(BxZone zone1, BxZone zone2) {
        if (zone1.getLines().size() != zone2.getLines().size()) {
			return false;
        }
		for (int i = 0; i < zone1.getLines().size(); i++) {
            if (!areEqual(zone1.getLines().get(i), zone2.getLines().get(i))) {
                return false;
            }
        }
        return true;
    }
    
    public static boolean areEqual(BxPage page1, BxPage page2) {
        if (page1.getZones().size() != page2.getZones().size()) {
			return false;
            
        }
		for (int i = 0; i < page1.getZones().size(); i++) {
            if (!areEqual(page1.getZones().get(i), page2.getZones().get(i))) {
                return false;
            }
        }
        return true;
    }
    
    public static boolean areEqual(BxDocument doc1, BxDocument doc2) {
		if (doc1.getPages().size() != doc2.getPages().size()) {
			return false;
        }
        for (int i = 0; i < doc1.getPages().size(); i++) {
            if (!areEqual(doc1.getPages().get(i), doc1.getPages().get(i))) {
                return false;
            }
        }
        return true;
	}
}
