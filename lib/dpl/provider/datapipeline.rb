module DPL
  class Provider
    class Datapipeline < Provider
      experimental 'AWS Datapipeline'
      """Implements AWS Datapipeline deployment
      """

      requires 'aws-sdk'
      requires 'json'

      DEFAULT_REGION = 'us-east-1'

      def access_key_id
        options[:access_key_id] || context.env['AWS_ACCESS_KEY_ID'] || raise(Error, "missing access_key_id")
      end

      def secret_access_key
        options[:secret_access_key] || context.env['AWS_SECRET_ACCESS_KEY'] || raise(Error, "missing secret_access_key")
      end

      def region
        options[:region] || DEFAULT_REGION
      end

      def pipeline_name
        options[:pipeline_name]
      end

      def pipeline_definition_file
        options[:pipeline_definition_file]
      end

      def pipeline_definition
        @pipeline_definition ||= JSON.parse(File.read(pipeline_definition_file))
      end

      def pipeline_tags
        options[:pipeline_tags]
      end

      def pipeline_description
        options[:pipeline_description]
      end

      def datapipeline
        @datapipeline ||= Aws::DataPipeline::Client.new
      end

      def check_auth
        log "Logging in with Access Key: #{access_key_id[-4..-1].rjust(20, '*')}"
        Aws.config[:credentials] = Aws::Credentials.new(access_key_id, secret_access_key)
        Aws.config.update(region: region)
      end

      def needs_key?
        false
      end

      def cleanup
      end

      def uncleanup
      end

      def push_app
        log "Deploying pipeline #{pipeline_name} with pipeline definition @ #{pipeline_definition_file}"

        pipelines = datapipeline.list_pipelines.pipeline_id_list.select { |x| x.name == pipeline_name }
        
        log "pipeline list size: #{pipelines.size}"

        if pipelines.size > 1
          error "Pipelines found '#{pipeline_name}': #{pipelines}"
        end

        if pipelines.size == 1
          log "Deleting pipeline #{pipelines.first.id}"
          datapipeline.delete_pipeline({
            pipeline_id: pipelines.first.id
          })
        end

        log "Processing tags..."
        tags = pipeline_tags.split(',').map { |x| { :key => x.split('=')[0], :value => x.split('=')[1] }}

        log "Creating pipeline #{pipeline_name}"

        response = datapipeline.create_pipeline({
          name: pipeline_name,
          unique_id: pipeline_name,
          description: pipeline_description,
          tags: tags
        })
        pipeline_id = response.pipeline_id
        log "Pipeline #{pipeline_id} created"

        log "Updating pipeline #{pipeline_id}"
        response = datapipeline.put_pipeline_definition({
          pipeline_id: pipeline_id,
          pipeline_objects: pipeline_objects,
          parameter_objects: parameter_objects,
          parameter_values: parameter_values
        })

        if response[:errored]
          log "Failed to put pipeline definition:"
          log response[:validation_errors].inspect
          error "Deployment failed."
        else
          log "Deployment successful."
        end

      end

      def pipeline_objects
        pipeline_objects = []

        pipeline_definition['objects'].each do |obj|
          current = {
            :id => obj.delete('id'),
            :name => obj.delete('name')
          }
          fields = []
          obj.each do |key, value|
            fields.push(convertComplexField(key,value))
          end
          current[:fields] = fields.flatten
          pipeline_objects.push current
        end
        pipeline_objects
      end

      def parameter_objects
        parameter_objects = []
        pipeline_definition['parameters'].each do |obj|
          current = {
            :id => obj.delete('id')
          }
          attributes = []
          obj.each do |key, value|
            attributes.push(convertComplexField(key,value))
          end
          current[:attributes] = attributes.flatten
          parameter_objects.push current
        end
        parameter_objects
      end

      def parameter_values
        parameter_values = []
        pipeline_definition['values'].each do |key,value|
          parameter_values.push(convertParameterValue(key,value))
        end
        parameter_values.flatten!
        parameter_values
      end

      def convertComplexField(key, value)
        values = []
        if value.kind_of?(Array)
          value.each do |item|
            values.push convertField(key, item)
          end
        else
          values.push convertField(key, value)
        end
        values
      end

      def convertField(key, value)
        field = { 'key': key }
        if value.is_a?(Hash) && value.keys == ['ref']
          field[:ref_value] = value['ref']
        else
          field[:string_value] = value.to_s
        end
        field
      end

      def convertParameterValue(key, value)
        values = []
        if value.kind_of?(Array)
          value.each do |item|
            current = { :id => key, :string_value => item}
            values.push current
          end
        else
          current = { :id => key, :string_value => value.to_s }
          values.push current
        end
        values
      end

    end
  end
end
