package pl.edu.icm.yadda.analysis.metadata.zoneclassification.features;

import pl.edu.icm.yadda.analysis.classification.features.FeatureCalculator;
import pl.edu.icm.yadda.analysis.textr.model.BxLine;
import pl.edu.icm.yadda.analysis.textr.model.BxPage;
import pl.edu.icm.yadda.analysis.textr.model.BxZone;

public class BracketedLineRelativeCount implements FeatureCalculator<BxZone, BxPage> {
	private static String featureName = "BracketedRelativeLineCount";
	
	@Override
	public String getFeatureName() {
		return featureName;
	}

	@Override
	public double calculateFeatureValue(BxZone zone, BxPage page) {
		int lines = 0;
		int bracketedLines = 0;
		
		for(BxLine line: zone.getLines()) {
			++lines;
			if(line.toText().charAt(0) == '[' || line.toText().charAt(0) == ']')
				++bracketedLines;
		}
		return (new Double(bracketedLines))/(new Double(lines));
	}
}