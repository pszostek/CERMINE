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

package pl.edu.icm.cermine.libsvm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.cli.*;

import pl.edu.icm.cermine.evaluation.tools.EvaluationUtils;
import pl.edu.icm.cermine.evaluation.tools.EvaluationUtils.DocumentsIterator;
import pl.edu.icm.cermine.exception.AnalysisException;
import pl.edu.icm.cermine.exception.TransformationException;
import pl.edu.icm.cermine.structure.HierarchicalReadingOrderResolver;
import pl.edu.icm.cermine.structure.SVMInitialZoneClassifier;
import pl.edu.icm.cermine.structure.SVMMetadataZoneClassifier;
import pl.edu.icm.cermine.structure.model.*;
import pl.edu.icm.cermine.tools.classification.features.FeatureVectorBuilder;
import pl.edu.icm.cermine.tools.classification.general.BxDocsToTrainingSamplesConverter;
import pl.edu.icm.cermine.tools.classification.general.TrainingSample;
import pl.edu.icm.cermine.tools.classification.sampleselection.SampleFilter;

import com.davidsoergel.conja.*;


public class LibSVMExporter {
	public HierarchicalReadingOrderResolver ror;
	public FeatureVectorBuilder<BxZone, BxPage> metaVectorBuilder;
	public FeatureVectorBuilder<BxZone, BxPage> initialVectorBuilder;
	public BufferedWriter svmMetaFile;
	public BufferedWriter svmInitialFile;
	public SampleFilter metaSamplesFilter;
	
	public LibSVMExporter(File inputDirFile) throws IOException {
		ror = new HierarchicalReadingOrderResolver();
		metaVectorBuilder = SVMMetadataZoneClassifier.getFeatureVectorBuilder();
	    initialVectorBuilder = SVMInitialZoneClassifier.getFeatureVectorBuilder();
		metaSamplesFilter = new SampleFilter(
				BxZoneLabelCategory.CAT_METADATA);
		FileWriter initialStream = new FileWriter("initial_"
				+ inputDirFile.getName() + ".dat");
		svmInitialFile = new BufferedWriter(initialStream);

		FileWriter metaStream = new FileWriter("meta_" + inputDirFile.getName()
				+ ".dat");
		svmMetaFile = new BufferedWriter(metaStream);
	}
	
    public static void toLibSVM(TrainingSample<BxZoneLabel> trainingElement, BufferedWriter fileWriter) throws IOException {
       	if(trainingElement.getLabel() == null) {
       		return;
       	}
       	synchronized(fileWriter) {
       		fileWriter.write(String.valueOf(trainingElement.getLabel().ordinal()));
       		fileWriter.write(" ");
        	
       		Integer featureCounter = 1;
       		for (Double value : trainingElement.getFeatureVector().getValues()) {
       			StringBuilder sb = new StringBuilder();
       			Formatter formatter = new Formatter(sb, Locale.US);
       			formatter.format("%d:%.5f", featureCounter++, value);
       			fileWriter.write(sb.toString());
       			fileWriter.write(" ");
       		}
       		fileWriter.write("\n");
       	}
    }
    
    public static void toLibSVM(List<TrainingSample<BxZoneLabel>> trainingElements, String filePath) throws IOException {
    	BufferedWriter svmDataFile = null;
        try {
            FileWriter fstream = new FileWriter(filePath);
            svmDataFile = new BufferedWriter(fstream);
            for (TrainingSample<BxZoneLabel> elem : trainingElements) {
            	if(elem.getLabel() == null) {
            		continue;
            	}
                svmDataFile.write(String.valueOf(elem.getLabel().ordinal()));
                svmDataFile.write(" ");

                Integer featureCounter = 1;
                for (Double value : elem.getFeatureVector().getValues()) {
                    StringBuilder sb = new StringBuilder();
                    Formatter formatter = new Formatter(sb, Locale.US);
                    formatter.format("%d:%.5f", featureCounter++, value);
                    svmDataFile.write(sb.toString());
                    svmDataFile.write(" ");
                }
                svmDataFile.write("\n");
            }
            svmDataFile.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return;
        } finally {
        	if(svmDataFile != null) {
        		svmDataFile.close();
        	}
        }
        
        System.out.println("Done.");
    }
    
    public void treatFile(File file) throws IOException, TransformationException, AnalysisException {
    	BxDocument doc = EvaluationUtils.getDocument(file);
    	String filename = doc.getFilename();
    	doc = ror.resolve(doc);
    	doc.setFilename(filename);

    	for (BxZone zone : doc.asZones()) {
    		if (zone.getLabel() != null) {
    			if (zone.getLabel().getCategory() != BxZoneLabelCategory.CAT_METADATA) {
    				zone.setLabel(zone.getLabel().getGeneralLabel());
    			}
    		}
    		else {
    			zone.setLabel(BxZoneLabel.OTH_UNKNOWN);
    		}
    	}
		List<TrainingSample<BxZoneLabel>> newMetaSamples = BxDocsToTrainingSamplesConverter
				.getZoneTrainingSamples(doc, metaVectorBuilder,
						BxZoneLabel.getIdentityMap());
		newMetaSamples = metaSamplesFilter.pickElements(newMetaSamples);
    	
		List<TrainingSample<BxZoneLabel>> newInitialSamples = BxDocsToTrainingSamplesConverter
				.getZoneTrainingSamples(doc, initialVectorBuilder,
						BxZoneLabel.getLabelToGeneralMap());
		
		for (TrainingSample<BxZoneLabel> sample : newMetaSamples) {
			toLibSVM(sample, svmMetaFile);
		}
		for (TrainingSample<BxZoneLabel> sample : newInitialSamples) {
			toLibSVM(sample, svmInitialFile);
		}
    }
    public static void main(String[] args) throws ParseException, IOException, TransformationException, AnalysisException, CloneNotSupportedException {
        Options options = new Options();

        CommandLineParser parser = new GnuParser();
        CommandLine line = parser.parse(options, args);

        if (args.length != 1) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(" [-options] input-directory", options);
            System.exit(1); 
        }
        String inputDirPath = line.getArgs()[0];
        File inputDirFile = new File(inputDirPath);

        Integer docIdx = 0;
		File[] files = inputDirFile.listFiles(new FilenameFilter(){
			@Override
			public boolean accept(File dir, String name) {
				if(name.endsWith(".cxml")) {
					return true;
				}
				return false;
			}
		});
		Set<File> filesSet = new HashSet<File>(Arrays.asList(files));
		final LibSVMExporter exporter = new LibSVMExporter(inputDirFile);

        Parallel.forEach(filesSet, new Function<File, Void>() {
        	public Void apply(File obj) {
        		System.out.println("Working on " + obj.getName());
        		try {
					exporter.treatFile(obj);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TransformationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (AnalysisException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
        	}
        });
		exporter.svmInitialFile.close();
		exporter.svmMetaFile.close();
    }
}