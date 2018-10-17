module BlocRecord
  class Collection < Array
    def update_all(updates)
      ids = self.map(&:id)
      self.any? ? self.first.class.update(ids, updates) : false
    end

    def take(num=1)
      self.any? ? self.first.class.take(num) : false
    end

    def where(*args)
      self.any? ? self.first.class.where(args) : false
    end

    def where_not(*args)
      self.any? ? self.first.class.where_not(args) : false
    end

    def destroy_all
      ids = self.map(&:id)
      self.any? ? self.first.class.destroy(ids) : false
    end
  end
end
