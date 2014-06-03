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

package pl.edu.icm.cermine.evaluation.tools;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import pl.edu.icm.cermine.exception.TransformationException;
import pl.edu.icm.cermine.structure.model.BxDocument;
import pl.edu.icm.cermine.structure.model.BxPage;
import pl.edu.icm.cermine.structure.transformers.TrueVizToBxDocumentReader;
import pl.edu.icm.cermine.tools.classification.general.DirExtractor;
import pl.edu.icm.cermine.tools.classification.general.DocumentsExtractor;

public class EvaluationUtils {
    public static List<BxDocument> getDocumentsFromPath(String inputDirPath) throws TransformationException
	{
		if (inputDirPath == null) {
			throw new NullPointerException("Input directory must not be null.");
		}

		if (!inputDirPath.endsWith(File.separator)) {
			inputDirPath += File.separator;
		}
		DocumentsExtractor extractor = new DirExtractor(inputDirPath);
		
		List<BxDocument> evaluationDocuments;
		evaluationDocuments = extractor.getDocuments();
		return evaluationDocuments;
	}
    
    public static BxDocument getDocument(File file) throws IOException, TransformationException {
    	TrueVizToBxDocumentReader tvReader = new TrueVizToBxDocumentReader();
        BxDocument newDoc = new BxDocument();
        InputStream is = new FileInputStream(file);
        try {
            List<BxPage> pages = tvReader.read(new InputStreamReader(is));
            for(BxPage page: pages) {
                page.setParent(newDoc);
            }
            newDoc.setFilename(file.getName());
            newDoc.setPages(pages);
            return newDoc;
        } finally {
            if (is != null) {
            	is.close();
            }
        }
    }
 
    public static class DocumentsIterator implements Iterable<BxDocument> {

    	private File dir;
    	private int curIdx;
    	private File[] files;

    	public DocumentsIterator(String dirPath) {
			if(!dirPath.endsWith(File.separator)) {
				dirPath += File.separator;
			}
    		this.dir = new File(dirPath);
    		this.curIdx = -1;
    		
    		this.files = dir.listFiles(new FilenameFilter(){
				@Override
				public boolean accept(File dir, String name) {
					if(name.endsWith(".xml")) {
						return true;
					}
					return false;
				}
    			
    		});
    	}
    	
		@Override
		public Iterator<BxDocument> iterator() {

	        return new Iterator<BxDocument>() {

	            @Override
	            public boolean hasNext() {
	                return curIdx + 1 < files.length;
	            }

	            @Override
	            public BxDocument next() {
	                ++curIdx;
        			try {
						return getDocument(files[curIdx]);
					} catch (IOException e) {
						return null;
					} catch (TransformationException e) {
						return null;
					}
	            }

	            @Override
	            public void remove() {
	                ++curIdx;
	            }
	        };
		}
    	
    }
   
}
