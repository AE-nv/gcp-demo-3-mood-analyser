/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package be.ae.googleclouddemo;

import be.ae.googleclouddemo.utils.CustomPipelineOptions;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Mean;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectPipelineRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowPipelineRunner
 */
public class GeneralMoodPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(GeneralMoodPipeline.class);

	public static void main(String[] args) {
		CustomPipelineOptions options =
	  		PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
		Pipeline p = Pipeline.create(options);

		p.apply(PubsubIO.Read.named("Read from analysed messages")
				// Read from PubSub
				.topic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
				.timestampLabel("ts")
				.withCoder(TableRowJsonCoder.of()))

			// Extract mood from analysed messages
			.apply(
				"Extract mood",
				ParDo.of(new DoFn<TableRow, Number>() {
					@Override
					public void processElement(ProcessContext c) throws Exception {
						c.output(Double.parseDouble(c.element().get("score").toString()));
					}
				}))

			// Init sliding window
			.apply(
				"Sliding window of 60 seconds, updated every 3 seconds",
					Window.into(FixedWindows.of(Duration.standardMinutes(1))))
			.apply("trigger",
					Window.<Number>triggering(
							AfterWatermark.pastEndOfWindow()
									.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1)))
									.withLateFirings(AfterPane.elementCountAtLeast(1)))
							.accumulatingFiredPanes()
							.withAllowedLateness(Duration.standardSeconds(1)))

			.apply("Calculate average",  Mean.globally().withoutDefaults())

			// Format mood for the output topic
			.apply("Format mood",
				ParDo.of(new DoFn<Number, TableRow>() {
					@Override
					public void processElement(ProcessContext c) throws Exception {
						TableRow r = new TableRow();
						r.set("average_mood", c.element());
						LOG.info("Outputting average mood {} at {} ", c.element(), new Date().getTime());
						c.output(r);
					}
				}))

			// Write mood to to sink
			.apply(PubsubIO.Write.named("Publish Mood")
				.topic(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic()))
				.withCoder(TableRowJsonCoder.of()));

		p.run();
	}
}
